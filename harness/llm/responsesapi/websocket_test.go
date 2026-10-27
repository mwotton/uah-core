package responsesapi

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/viktordanov/uah-core/harness/llm"
	"github.com/viktordanov/uah-core/harness/primitives"
)

// wsServer is a fake Responses endpoint on both transports: an upgrade
// gets response.create messages, a POST the HTTP stream. Response n answers
// with one message, "reply n". Over the WebSocket it keeps each response
// for its connection, so it can check a request that continues one.
type wsServer struct {
	*httptest.Server

	mu sync.Mutex
	// upgrade, when set, is the status that answers an upgrade.
	upgrade int
	// answer, when set, says how to answer response n.
	answer func(n int) wsAnswer
	n      int
	conns  int
	posts  int
	sends  []wsCreate
	// open counts the connections open but not upgraded; hosts are the
	// upgrades' Host headers.
	open  atomic.Int64
	hosts []string
}

// wsAnswer is how the server answers one response.create.
type wsAnswer struct {
	// Fail answers with response.failed; Error with an error event with
	// this code and Status.
	Fail   bool
	Error  string
	Status int
	// Hold waits until it closes, or the connection does, before the
	// answer.
	Hold chan struct{}
	// Drop closes the connection without an answer; CloseAfter closes it
	// after the answer.
	Drop, CloseAfter bool
}

// wsCreate is what one response.create carried.
type wsCreate struct {
	Conn      int
	Model     string
	Previous  string
	Input     []jsontext.Value
	Full      int // the input items with the previous response's
	TurnState string
}

func newWSServer(t *testing.T) *wsServer {
	t.Helper()
	s := &wsServer{}
	s.Server = httptest.NewUnstartedServer(s)
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			s.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			s.open.Add(-1)
		default:
		}
	}
	s.Start()
	t.Cleanup(s.Close)

	return s
}

func (s *wsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") == "" {
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.n++
		s.posts++
		n := s.n
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range answerEvents(n) {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
		}

		return
	}
	s.mu.Lock()
	upgrade := s.upgrade
	s.mu.Unlock()
	if upgrade != 0 {
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(upgrade)

		return
	}
	if r.Header.Get("OpenAI-Beta") != webSocketBeta {
		http.Error(w, "no beta header", http.StatusBadRequest)

		return
	}
	s.mu.Lock()
	s.hosts = append(s.hosts, r.Host)
	s.mu.Unlock()
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionNoContextTakeover})
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()
	c.SetReadLimit(-1)
	s.mu.Lock()
	s.conns++
	conn := s.conns
	s.mu.Unlock()
	kept := map[string][]jsontext.Value{}
	for {
		_, data, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var create struct {
			Type     string            `json:"type"`
			Model    string            `json:"model"`
			Previous string            `json:"previous_response_id"`
			Input    []jsontext.Value  `json:"input"`
			Metadata map[string]string `json:"client_metadata"`
		}
		if err := json.Unmarshal(data, &create); err != nil || create.Type != "response.create" {
			return
		}
		full := create.Input
		if create.Previous != "" {
			prev, ok := kept[create.Previous]
			if !ok {
				_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"not found"}}`))

				continue
			}
			full = slices.Concat(prev, create.Input)
		}
		s.mu.Lock()
		s.n++
		n := s.n
		s.sends = append(s.sends, wsCreate{Conn: conn, Model: create.Model, Previous: create.Previous, Input: create.Input, Full: len(full), TurnState: create.Metadata[clientMetadataTurnKey]})
		var answer wsAnswer
		if s.answer != nil {
			answer = s.answer(n)
		}
		s.mu.Unlock()
		if answer.Hold != nil {
			select {
			case <-answer.Hold:
			case <-r.Context().Done():
				return
			}
		}
		switch {
		case answer.Drop:
			return
		case answer.Error != "":
			_ = c.Write(r.Context(), websocket.MessageText, fmt.Appendf(nil, `{"type":"error","status":%d,"error":{"code":%q,"message":"fake"}}`, answer.Status, answer.Error))

			continue
		case answer.Fail:
			_ = c.Write(r.Context(), websocket.MessageText, fmt.Appendf(nil, `{"type":"response.failed","response":{"id":"resp_%d","object":"response","status":"failed","output":[],"error":{"code":"server_error","message":"fake"}}}`, n))

			continue
		}
		for _, event := range answerEvents(n) {
			if c.Write(r.Context(), websocket.MessageText, event) != nil {
				return
			}
		}
		kept[fmt.Sprintf("resp_%d", n)] = append(full, answerItem(n))
		if answer.CloseAfter {
			return
		}
	}
}

func answerItem(n int) jsontext.Value {
	return jsontext.Value(fmt.Sprintf(`{"type":"message","id":"msg_%d","role":"assistant","status":"completed","content":[{"type":"output_text","text":"reply %d","annotations":[]}]}`, n, n))
}

// answerEvents are response n's events, its output only in the item event,
// after the events that stand for headers, as the ChatGPT backend sends
// them.
func answerEvents(n int) [][]byte {
	return [][]byte{
		[]byte(`{"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":true}}`),
		fmt.Appendf(nil, `{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"ts-%d"}}`, n),
		fmt.Appendf(nil, `{"type":"response.created","response":{"id":"resp_%d","object":"response","status":"in_progress","output":[]}}`, n),
		fmt.Appendf(nil, `{"type":"response.output_item.done","output_index":0,"item":%s}`, answerItem(n)),
		fmt.Appendf(nil, `{"type":"response.completed","response":{"id":"resp_%d","object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, n),
	}
}

func (s *wsServer) creates() []wsCreate {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.sends)
}

// wsClient is an adapter over a WebSocketTransport to s, and what the
// transport reported.
type wsClient struct {
	llm.Adapter

	ws    *WebSocketTransport
	mu    sync.Mutex
	sends []WebSocketSend
}

func newWSClient(t *testing.T, s *wsServer) *wsClient {
	t.Helper()
	c := &wsClient{}
	c.ws = &WebSocketTransport{HTTP: http.DefaultTransport, ResponseHeaderTimeout: 5 * time.Second, Sent: func(_ *http.Request, send WebSocketSend) {
		c.mu.Lock()
		c.sends = append(c.sends, send)
		c.mu.Unlock()
	}}
	t.Cleanup(func() { _ = c.ws.Close() })
	remote := primitives.NewRemoteClientWithHTTPClient(&http.Client{Transport: c.ws})
	t.Cleanup(func() { _ = remote.Close() })
	adapter, err := NewAdapter(remote, Config{Endpoint: s.URL + "/responses", MaxAttempts: new(1)})
	if err != nil {
		t.Fatal(err)
	}
	c.Adapter = adapter

	return c
}

func (c *wsClient) reported() []WebSocketSend {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.sends)
}

func userItem(text string) llm.Item {
	return llm.Item{Type: llm.ItemMessage, Data: llm.Message{Role: llm.RoleUser, Text: text}}
}

// turn sends history with a user message added and returns the history
// with the answer.
func (c *wsClient) turn(t *testing.T, model string, history []llm.Item, text string) []llm.Item {
	t.Helper()
	history = append(slices.Clone(history), userItem(text))
	resp, err := c.Respond(t.Context(), llm.Request{Model: llm.Model{ID: model}, Input: history}, llm.RequestOptions{})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	return append(history, resp.Output...)
}

func TestWebSocketContinuesTheLastResponse(t *testing.T) {
	s := newWSServer(t)
	c := newWSClient(t, s)
	history := c.turn(t, "m", nil, "one")
	history = c.turn(t, "m", history, "two")
	c.turn(t, "m", history, "three")

	creates := s.creates()
	if len(creates) != 3 {
		t.Fatalf("creates = %+v", creates)
	}
	if creates[0].Previous != "" || len(creates[0].Input) != 1 {
		t.Fatalf("first = %+v, want the whole input", creates[0])
	}
	for i, want := range []string{"resp_1", "resp_2"} {
		got := creates[i+1]
		if got.Previous != want || len(got.Input) != 1 || got.Full != 2*(i+2)-1 || got.Conn != 1 {
			t.Fatalf("send %d = %+v, want only the new message after %s on one connection", i+2, got, want)
		}
	}
	sends := c.reported()
	if len(sends) != 3 || sends[0].Incremental || sends[0].Reason != SendNewConnection || !sends[1].Incremental || !sends[2].Incremental || sends[0].Bytes <= 0 {
		t.Fatalf("reported = %+v", sends)
	}
}

func TestWebSocketSendsTheWholeInputWhenTheRequestDoesNotContinue(t *testing.T) {
	s := newWSServer(t)
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	two := c.turn(t, "other", one, "two")            // the model changed
	c.turn(t, "other", one, "two again")             // a rewind: the history lost two's answer
	c.turn(t, "other", one[:1], "edited")            // and lost one's answer
	c.turn(t, "other", slices.Concat(two, one), "x") // and an input that does not follow

	var reasons []string
	for _, send := range c.reported()[1:] {
		if send.Incremental {
			t.Fatalf("reported = %+v", c.reported())
		}
		reasons = append(reasons, send.Reason)
	}
	if want := []string{SendRequestChanged, SendInputChanged, SendInputChanged, SendInputChanged}; !slices.Equal(reasons, want) {
		t.Fatalf("reasons = %v, want %v", reasons, want)
	}
	for _, create := range s.creates() {
		if create.Previous != "" {
			t.Fatalf("creates = %+v", s.creates())
		}
	}
}

func TestWebSocketSendsTheWholeInputWhenAnAnswerChanged(t *testing.T) {
	s := newWSServer(t)
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	edited := slices.Clone(one)
	answer, ok := edited[1].Data.(llm.Message)
	if !ok {
		t.Fatalf("history = %+v", one)
	}
	answer.Text = "an edited reply"
	edited[1].Data = answer // under the same ID
	c.turn(t, "m", edited, "two")

	if creates := s.creates(); len(creates) != 2 || creates[1].Previous != "" {
		t.Fatalf("creates = %+v", creates)
	}
	if got := c.reported()[1]; got.Incremental || got.Reason != SendInputChanged {
		t.Fatalf("second = %+v", got)
	}
}

func TestWebSocketContinuationComparesWhatItemsSay(t *testing.T) {
	refusal := `{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}`
	asText := `{"content":[{"annotations":[],"logprobs":[],"text":"no","type":"output_text"}],"id":"msg_1","role":"assistant","status":"completed","type":"message"}`
	server, _ := readItem([]byte(refusal))
	edited, _ := readItem([]byte(strings.Replace(refusal, `"no"`, `"never"`, 1)))
	ours, _ := readItem([]byte(asText))
	if !server.equal(server) || server.equal(edited) || server.equal(ours) {
		t.Fatalf("a refusal compares equal to an edited one or to text: %+v %+v %+v", server, edited, ours)
	}
	call := `{"type":"function_call","id":"fc_1","call_id":"c","name":"Bash","arguments":"{\"command\":\"ls\"}","status":"completed"}`
	sent := `{"arguments":"{\"command\":\"ls\"}","call_id":"c","id":"fc_1","name":"Bash","type":"function_call"}`
	a, _ := readItem([]byte(call))
	b, _ := readItem([]byte(sent))
	if !a.equal(b) {
		t.Fatalf("the server's call and the request's differ: %+v %+v", a, b)
	}

	withMetadata, err := parseWSRequest([]byte(`{"model":"m","client_metadata":{"a":"b"},"input":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame, _, _ := withMetadata.frame(nil, "ts-1"); !strings.Contains(string(frame), `"client_metadata":{"a":"b","x-codex-turn-state":"ts-1"}`) {
		t.Fatalf("frame = %s, want the turn state merged into the body's metadata", frame)
	}
	nullMetadata, err := parseWSRequest([]byte(`{"model":"m","client_metadata":null,"input":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame, _, _ := nullMetadata.frame(nil, "ts-1"); !strings.Contains(string(frame), `"client_metadata":{"x-codex-turn-state":"ts-1"}`) {
		t.Fatalf("frame = %s", frame)
	}

	body := []byte(`{"model":"m","previous_response_id":"resp_0","input":[{"role":"user","content":"hi"}]}`)
	r, err := parseWSRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	cont := &continuation{id: "resp_1", fields: r.fields, input: r.input[:0], output: []outputItem{}}
	if frame, incremental, _ := r.frame(cont, ""); incremental || strings.Count(string(frame), "previous_response_id") != 1 {
		t.Fatalf("frame = %s, want the body's own previous_response_id only", frame)
	}
}

func TestWebSocketErrorKeepsItsRetryAfter(t *testing.T) {
	for _, event := range []string{
		`{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"slow","headers":{"retry-after":"7"}}}`,
		`{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","message":"slow"},"headers":{"retry-after":"7"}}`,
	} {
		resp := errorEvent([]byte(event)).response(nil)
		if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "7" {
			t.Fatalf("%s: response = %d %v", event, resp.StatusCode, resp.Header)
		}
	}
}

func TestWebSocketClosedWhileDialingSendsOverHTTP(t *testing.T) {
	s := newWSServer(t)
	dialing, release := make(chan struct{}), make(chan struct{})
	ws := &WebSocketTransport{HTTP: http.DefaultTransport, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		close(dialing)
		<-release

		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	body, err := requestBody(validRequest(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+"/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		resp, err := ws.RoundTrip(withBody(req, body))
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		done <- err
	}()
	<-dialing
	_ = ws.Close()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	posts := s.posts
	s.mu.Unlock()
	ws.mu.Lock()
	conn := ws.conn
	ws.mu.Unlock()
	if posts != 1 || conn != nil || len(s.creates()) != 0 {
		t.Fatalf("posts = %d, conn = %v, creates = %+v: want the request over HTTP and no connection", posts, conn, s.creates())
	}
}

func TestWebSocketSendsTheWholeInputAfterAFailedResponse(t *testing.T) {
	s := newWSServer(t)
	s.answer = func(n int) wsAnswer { return wsAnswer{Fail: n == 2} }
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	resp, err := c.Respond(t.Context(), llm.Request{Model: llm.Model{ID: "m"}, Input: append(slices.Clone(one), userItem("two"))}, llm.RequestOptions{})
	if err != nil || resp.Failure == nil {
		t.Fatalf("response = %+v, error = %v, want a failure", resp, err)
	}
	c.turn(t, "m", one, "two")

	creates := s.creates()
	if len(creates) != 3 || creates[1].Previous != "resp_1" || creates[2].Previous != "" || creates[2].Conn != 1 {
		t.Fatalf("creates = %+v", creates)
	}
	if got := c.reported()[2]; got.Incremental || got.Reason != SendNotCompleted {
		t.Fatalf("third = %+v", got)
	}
}

func TestWebSocketResendsWhenTheServerLostTheResponse(t *testing.T) {
	s := newWSServer(t)
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	// The server forgets resp_1: a new connection has none of the old
	// one's responses.
	s.mu.Lock()
	s.answer = func(int) wsAnswer { return wsAnswer{} }
	s.mu.Unlock()
	c.ws.mu.Lock()
	c.ws.last.id = "resp_gone"
	c.ws.mu.Unlock()
	c.turn(t, "m", one, "two")

	creates := s.creates()
	if len(creates) != 2 || creates[1].Previous != "" || creates[1].Conn != 2 || len(creates[1].Input) != 3 {
		t.Fatalf("creates = %+v, want the whole input on a new connection", creates)
	}
	sends := c.reported()
	if len(sends) != 3 || !sends[1].Incremental || sends[2].Incremental || sends[2].Reason != SendNotFound {
		t.Fatalf("reported = %+v", sends)
	}
}

func TestWebSocketReconnectsAfterTheServerClosed(t *testing.T) {
	s := newWSServer(t)
	s.answer = func(n int) wsAnswer { return wsAnswer{CloseAfter: n == 1} }
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.ws.mu.Lock()
		dead := c.ws.conn == nil || c.ws.conn.dead()
		c.ws.mu.Unlock()
		if dead || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.turn(t, "m", one, "two")

	creates := s.creates()
	if len(creates) != 2 || creates[1].Conn != 2 || creates[1].Previous != "" {
		t.Fatalf("creates = %+v", creates)
	}
	if got := c.reported()[1]; got.Incremental || got.Reason != SendConnectionClosed {
		t.Fatalf("second = %+v", got)
	}
}

func TestWebSocketFallsBackToHTTPOnUpgradeRequired(t *testing.T) {
	s := newWSServer(t)
	s.upgrade = http.StatusUpgradeRequired
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	c.turn(t, "m", one, "two")

	s.mu.Lock()
	posts := s.posts
	s.mu.Unlock()
	if posts != 2 {
		t.Fatalf("posts = %d", posts)
	}
	sends := c.reported()
	if len(sends) != 2 || !sends[0].HTTP || sends[0].Reason != SendUpgradeRequired || !sends[1].HTTP || sends[1].Reason != SendUpgradeRequired {
		t.Fatalf("reported = %+v", sends)
	}
}

// A refused upgrade answers with its status, a redirect not followed, and
// leaves no connection open.
func TestWebSocketRefusedUpgradeLeavesNoConnection(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTemporaryRedirect} {
		s := newWSServer(t)
		s.upgrade = status
		ws := &WebSocketTransport{HTTP: http.DefaultTransport}
		body, err := requestBody(validRequest(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+"/responses", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := ws.RoundTrip(withBody(req, body))
		if err != nil || resp.StatusCode != status {
			t.Fatalf("response = %v, error = %v, want %d", resp, err, status)
		}
		_ = resp.Body.Close()
		for deadline := time.Now().Add(5 * time.Second); s.open.Load() != 0; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%d: %d connections still open", status, s.open.Load())
			}
		}
	}
}

func TestWebSocketFallsBackToHTTPAfterRepeatedFailures(t *testing.T) {
	s := newWSServer(t)
	s.answer = func(int) wsAnswer { return wsAnswer{Drop: true} }
	c := newWSClient(t, s)
	for range webSocketMaxFailures {
		if _, err := c.Respond(t.Context(), validRequest(), llm.RequestOptions{}); err == nil {
			t.Fatal("a dropped connection returned no error")
		}
	}
	c.turn(t, "m", nil, "one")

	if len(s.creates()) != webSocketMaxFailures {
		t.Fatalf("creates = %+v", s.creates())
	}
	if got := c.reported()[webSocketMaxFailures]; !got.HTTP || got.Reason != SendWebSocketFailed {
		t.Fatalf("last = %+v", got)
	}
}

func TestWebSocketCancellationDropsTheResponse(t *testing.T) {
	s := newWSServer(t)
	hold := make(chan struct{})
	defer close(hold)
	s.answer = func(n int) wsAnswer {
		if n == 2 {
			return wsAnswer{Hold: hold}
		}

		return wsAnswer{}
	}
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := c.Respond(ctx, llm.Request{Model: llm.Model{ID: "m"}, Input: append(slices.Clone(one), userItem("two"))}, llm.RequestOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	c.turn(t, "m", one, "two")

	creates := s.creates()
	if len(creates) != 3 || creates[1].Previous != "resp_1" || creates[2].Previous != "" || creates[2].Conn != 2 {
		t.Fatalf("creates = %+v, want the interrupted response not continued", creates)
	}
	if got := c.reported()[2]; got.Incremental || got.Reason != SendInterrupted {
		t.Fatalf("third = %+v", got)
	}
	c.ws.mu.Lock()
	failures := c.ws.failures
	c.ws.mu.Unlock()
	if failures != 0 {
		t.Fatalf("failures = %d, a cancellation is not one", failures)
	}
}

func TestWebSocketSendsAConcurrentRequestOverHTTP(t *testing.T) {
	s := newWSServer(t)
	hold := make(chan struct{})
	s.answer = func(n int) wsAnswer {
		if n == 1 {
			return wsAnswer{Hold: hold}
		}

		return wsAnswer{}
	}
	c := newWSClient(t, s)
	done := make(chan error, 1)
	go func() {
		_, err := c.Respond(t.Context(), validRequest(), llm.RequestOptions{})
		done <- err
	}()
	for len(s.creates()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := c.Respond(t.Context(), validRequest(), llm.RequestOptions{}); err != nil {
		t.Fatal(err)
	}
	close(hold)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	sends := c.reported()
	if len(sends) != 2 || sends[0].HTTP || !sends[1].HTTP || sends[1].Reason != SendBusy {
		t.Fatalf("reported = %+v", sends)
	}
}

func TestWebSocketSendsTheTurnStateAsMetadata(t *testing.T) {
	s := newWSServer(t)
	ws := &WebSocketTransport{HTTP: http.DefaultTransport}
	t.Cleanup(func() { _ = ws.Close() })
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+"/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := requestBody(validRequest(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = withBody(req, body)
	req.Header.Set(headerCodexTurnState, "ts-1")
	resp, err := ws.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if creates := s.creates(); len(creates) != 1 || creates[0].TurnState != "ts-1" {
		t.Fatalf("creates = %+v", creates)
	}
	if got := resp.Header.Get(headerCodexTurnState); got != "ts-1" {
		t.Fatalf("the response's turn state = %q, want the metadata event's", got)
	}
}

func TestWebSocketAnswersAnErrorWithItsStatus(t *testing.T) {
	s := newWSServer(t)
	s.answer = func(int) wsAnswer { return wsAnswer{Error: "usage_limit_reached", Status: http.StatusTooManyRequests} }
	c := newWSClient(t, s)
	_, err := c.Respond(t.Context(), validRequest(), llm.RequestOptions{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusTooManyRequests || apiErr.Code != "usage_limit_reached" {
		t.Fatalf("error = %v", err)
	}
	c.ws.mu.Lock()
	failures := c.ws.failures
	c.ws.mu.Unlock()
	if failures != 0 {
		t.Fatalf("failures = %d, an error answer is not one", failures)
	}
}

func TestWebSocketUpgradesWithTheRequestsHost(t *testing.T) {
	s := newWSServer(t)
	ws := &WebSocketTransport{HTTP: http.DefaultTransport}
	t.Cleanup(func() { _ = ws.Close() })
	body, err := requestBody(validRequest(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"", "virtual.test", "virtual.test", "other.test"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+"/responses", nil)
		if err != nil {
			t.Fatal(err)
		}
		req = withBody(req, body)
		req.Host = host
		resp, err := ws.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	s.mu.Lock()
	hosts := slices.Clone(s.hosts)
	s.mu.Unlock()
	if want := []string{strings.TrimPrefix(s.URL, "http://"), "virtual.test", "other.test"}; !slices.Equal(hosts, want) {
		t.Fatalf("upgrades' hosts = %v, want %v: one connection for each host", hosts, want)
	}
}

func TestWebSocketCloseIdleConnections(t *testing.T) {
	s := newWSServer(t)
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	c.ws.CloseIdleConnections()
	c.turn(t, "m", one, "two")

	if creates := s.creates(); len(creates) != 2 || creates[1].Conn != 2 || creates[1].Previous != "" {
		t.Fatalf("creates = %+v", creates)
	}
	if got := c.reported()[1]; got.Reason != SendIdle {
		t.Fatalf("second = %+v", got)
	}
}

func TestWebSocketIdleConnectionCloses(t *testing.T) {
	old := webSocketIdleTimeout
	webSocketIdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { webSocketIdleTimeout = old })
	s := newWSServer(t)
	c := newWSClient(t, s)
	one := c.turn(t, "m", nil, "one")
	time.Sleep(200 * time.Millisecond)
	c.turn(t, "m", one, "two")

	if creates := s.creates(); len(creates) != 2 || creates[1].Conn != 2 || creates[1].Previous != "" {
		t.Fatalf("creates = %+v", creates)
	}
	if got := c.reported()[1]; got.Reason != SendIdle {
		t.Fatalf("second = %+v", got)
	}
}

// BenchmarkWebSocketFrame is what the transport adds to a request that
// continues the last response: splitting the body, checking that it
// extends the last request and its output, and building the message.
func BenchmarkWebSocketFrame(b *testing.B) {
	for _, turns := range []int{100, 500} {
		request := benchmarkRequest(turns)
		body, err := requestBody(request, "cache-key", nil)
		if err != nil {
			b.Fatal(err)
		}
		// The last request ended at the last user message; its response
		// was the reasoning and the call after it.
		previous := request
		previous.Input = request.Input[:len(request.Input)-4]
		previousBody, err := requestBody(previous, "cache-key", nil)
		if err != nil {
			b.Fatal(err)
		}
		last, err := parseWSRequest(previousBody)
		if err != nil {
			b.Fatal(err)
		}
		var output []outputItem
		for _, item := range request.Input[len(previous.Input) : len(previous.Input)+2] {
			encoded, err := requestInputItem(item, nil)
			if err != nil {
				b.Fatal(err)
			}
			read, _ := readItem(encoded)
			output = append(output, read)
		}
		cont := &continuation{id: "resp_1", fields: last.fields, input: last.input, output: output}
		b.Run(fmt.Sprintf("turns=%d", turns), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				r, err := parseWSRequest(body)
				if err != nil {
					b.Fatal(err)
				}
				if _, incremental, reason := r.frame(cont, ""); !incremental {
					b.Fatalf("the request does not continue: %s", reason)
				}
			}
		})
	}
}
