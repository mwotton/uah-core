package responsesapi

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// The Responses API's WebSocket mode, as Codex uses it with the ChatGPT
// backend (openai/codex codex-rs at c587d6e: codex-api/src/endpoint/
// responses_websocket.rs, core/src/client.rs). A connection carries one
// response at a time: a request is one response.create message, the
// request body's fields with "type" added, and the server answers with the
// events the HTTP stream carries. A request that extends the last one, with
// the same fields and an input that starts with the last input and the last
// response's output, sends previous_response_id and only the input after
// them; with store false the server keeps that response only for the
// connection.

// WebSocketTransport sends Responses requests over one WebSocket and
// answers each with the server's events as an SSE stream, so the adapter,
// and any transport above it, read what HTTP gives them. It is meant for
// one conversation: a request from another history sends its whole input
// and resets the continuation. It sends over HTTP a request that arrives
// while another is in flight, and every request after the server refused
// the upgrade (426) or the WebSocket failed webSocketMaxFailures times in a
// row.
type WebSocketTransport struct {
	// HTTP sends the requests the WebSocket does not.
	HTTP http.RoundTripper
	// DialContext dials the connections; nil uses a net.Dialer.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)
	// ResponseHeaderTimeout bounds sending a request, and then the wait for
	// its first event, as http.Transport's bounds the wait for headers (0:
	// no limit).
	ResponseHeaderTimeout time.Duration
	// Sent, when set, is told how each request went out.
	Sent func(*http.Request, WebSocketSend)

	mu       sync.Mutex
	inUse    bool
	closed   bool
	conn     *wsConn
	last     *continuation
	lost     string // why the last continuation was dropped, for the next full send
	failures int
	fallback string // why every request goes over HTTP
}

// WebSocketSend is how a request went out.
type WebSocketSend struct {
	// HTTP says the request went over HTTP; Reason says why.
	HTTP bool
	// Incremental says the request continued the last response:
	// previous_response_id and only the new input. Else it sent its whole
	// input, and Reason says why it did not continue.
	Incremental bool
	// Bytes are what sending the request wrote to the network, compressed,
	// with the framing and TLS; 0 over HTTP.
	Bytes  int64
	Reason string
}

// The reasons of a WebSocketSend.
const (
	// The request went over HTTP.
	SendBusy            = "busy"
	SendUpgradeRequired = "upgrade_required"
	SendWebSocketFailed = "websocket_failed"
	SendClosed          = "closed"
	SendUnreadable      = "unreadable_body"
	// The request sent its whole input: no response to continue, a new
	// connection, or a request that does not extend the last one.
	SendNoPrevious     = "no_previous_response"
	SendNewConnection  = "new_connection"
	SendRequestChanged = "request_changed"
	SendInputChanged   = "input_changed"
	// The last continuation was dropped: the connection closed, was idle,
	// or did not match the request's headers; the last response was
	// interrupted or did not complete; or the server lost it.
	SendConnectionClosed = "connection_closed"
	SendIdle             = "idle"
	SendHeadersChanged   = "headers_changed"
	SendInterrupted      = "interrupted"
	SendNotCompleted     = "not_completed"
	SendNotFound         = "previous_response_not_found"
	SendConnectionLimit  = "websocket_connection_limit_reached"
)

const (
	webSocketBeta         = "responses_websockets=2026-02-06"
	webSocketConnectTime  = 15 * time.Second
	webSocketMaxFailures  = 3
	webSocketQueue        = 64
	headerCodexTurnState  = "X-Codex-Turn-State"
	clientMetadataTurnKey = "x-codex-turn-state"
)

// webSocketIdleTimeout closes a connection no request used for this long.
var webSocketIdleTimeout = 10 * time.Minute

var (
	errHeaderTimeout   = errors.New("timeout awaiting the first response event")
	errUpgradeRequired = errors.New("the server refused the WebSocket upgrade")
	errClosed          = errors.New("the WebSocket transport is closed")
)

var _ http.RoundTripper = (*WebSocketTransport)(nil)

// CloseIdleConnections closes the connection unless a request uses it, and
// the HTTP transport's idle ones, as http.Client.CloseIdleConnections asks.
func (t *WebSocketTransport) CloseIdleConnections() {
	t.mu.Lock()
	c := t.conn
	if t.inUse {
		c = nil
	} else if c != nil {
		t.conn, t.last, t.lost = nil, nil, SendIdle
	}
	t.mu.Unlock()
	if c != nil {
		c.close()
	}
	if idle, ok := t.HTTP.(interface{ CloseIdleConnections() }); ok {
		idle.CloseIdleConnections()
	}
}

// Close closes the connection; later requests go over HTTP.
func (t *WebSocketTransport) Close() error {
	t.mu.Lock()
	c := t.conn
	t.closed, t.conn, t.last = true, nil, nil
	t.mu.Unlock()
	if c != nil {
		c.close()
	}

	return nil
}

func (t *WebSocketTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	reason := t.fallback
	switch {
	case req.Method != http.MethodPost || req.Body == nil || req.Header.Get("Content-Encoding") != "":
		reason = SendUnreadable
	case t.closed:
		reason = SendClosed
	case reason == "" && t.inUse:
		reason = SendBusy
	}
	if reason != "" {
		t.mu.Unlock()
		t.report(req, WebSocketSend{HTTP: true, Reason: reason})

		return t.HTTP.RoundTrip(req)
	}
	t.inUse = true
	t.mu.Unlock()

	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		t.release()

		return nil, fmt.Errorf("read the request body: %w", err)
	}
	parsed, err := parseWSRequest(body)
	if err != nil {
		t.release()
		t.report(req, WebSocketSend{HTTP: true, Reason: SendUnreadable})

		return t.HTTP.RoundTrip(withBody(req, body))
	}
	resp, err := t.send(req, parsed)
	switch {
	case errors.Is(err, errUpgradeRequired):
		t.report(req, WebSocketSend{HTTP: true, Reason: SendUpgradeRequired})

		return t.HTTP.RoundTrip(withBody(req, body))
	case errors.Is(err, errClosed):
		t.report(req, WebSocketSend{HTTP: true, Reason: SendClosed})

		return t.HTTP.RoundTrip(withBody(req, body))
	}

	return resp, err
}

// send sends the request on the connection, once more on a new one when
// the server lost the last response or ended the connection. It holds the
// transport (inUse); the response's body releases it, or send when there
// is no body.
func (t *WebSocketTransport) send(req *http.Request, r wsRequest) (*http.Response, error) {
	key, header := handshake(req)
	turnState := req.Header.Get(headerCodexTurnState)
	trace := httptrace.ContextClientTrace(req.Context())
	for resent := false; ; resent = true {
		c, cont, lost, err := t.connection(req, key, header)
		if err != nil {
			return t.failed(req, nil, err)
		}
		if trace != nil && trace.GotConn != nil {
			trace.GotConn(httptrace.GotConnInfo{Reused: !c.fresh})
		}
		frame, incremental, reason := r.frame(cont, turnState)
		if !incremental && reason == "" {
			reason = or(lost, SendNoPrevious)
		}
		before := c.written.Load()
		if err := c.write(req.Context(), frame, t.ResponseHeaderTimeout); err != nil {
			return t.failed(req, c, err)
		}
		t.report(req, WebSocketSend{Incremental: incremental, Bytes: c.written.Load() - before, Reason: reason})
		if trace != nil && trace.WroteRequest != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
		first, err := c.next(req.Context(), t.ResponseHeaderTimeout)
		if err != nil {
			return t.failed(req, c, err)
		}
		if trace != nil && trace.GotFirstResponseByte != nil {
			trace.GotFirstResponseByte()
		}
		h := http.Header{}
		if c.fresh {
			h = c.header.Clone()
		}
		lead := [][]byte{first}
		for leading(first, h) {
			if first, err = c.next(req.Context(), t.ResponseHeaderTimeout); err != nil {
				return t.failed(req, c, err)
			}
			lead = append(lead, first)
		}
		failure := errorEvent(first)
		if code := failure.code(); !resent && (code == SendNotFound || code == SendConnectionLimit) {
			t.drop(c, code)

			continue
		}
		b := &wsBody{t: t, c: c, ctx: req.Context(), r: r}
		if failure != nil && failure.Status >= http.StatusBadRequest {
			b.finish(nil, false, false, SendNotCompleted)

			return failure.response(req), nil
		}
		h.Set("Content-Type", "text/event-stream")
		c.fresh = false
		for _, data := range lead {
			b.event(data)
		}

		return &http.Response{
			Status: "200 OK", StatusCode: http.StatusOK, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header: h, Body: b, ContentLength: -1, Request: req,
		}, nil
	}
}

// connection returns the open connection for the request, with the
// continuation it may use, or a new one; and why the last continuation was
// dropped, if it was.
func (t *WebSocketTransport) connection(req *http.Request, key string, header http.Header) (*wsConn, *continuation, string, error) {
	t.mu.Lock()
	c, cont := t.conn, t.last
	if c != nil {
		if c.idle != nil {
			c.idle.Stop()
		}
		switch {
		case c.dead():
			t.lost = SendConnectionClosed
		case c.key != key:
			t.lost = SendHeadersChanged
		default:
			lost := t.lost
			t.lost = ""
			t.mu.Unlock()

			return c, cont, lost, nil
		}
		t.conn, t.last = nil, nil
		c.close()
	}
	lost := t.lost
	t.mu.Unlock()
	c, err := dialWS(req, key, header, t.DialContext)
	if err != nil {
		return nil, nil, "", err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		c.close()

		return nil, nil, "", errClosed
	}
	t.conn, t.last, t.lost = c, nil, ""
	t.mu.Unlock()

	return c, nil, or(lost, SendNewConnection), nil
}

// drop closes the connection, which lost its continuation for reason.
func (t *WebSocketTransport) drop(c *wsConn, reason string) {
	c.close()
	t.mu.Lock()
	if t.conn == c {
		t.conn, t.last = nil, nil
	}
	t.lost = reason
	t.mu.Unlock()
}

// failed ends a request that failed before its response, on c when it got
// that far: a handshake's status answers as HTTP would, a canceled request
// with its context's error, and any other failure counts toward falling
// back to HTTP.
func (t *WebSocketTransport) failed(req *http.Request, c *wsConn, err error) (*http.Response, error) {
	if c != nil {
		t.drop(c, SendInterrupted)
	}
	status, _ := errors.AsType[*handshakeStatus](err)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inUse = false
	switch {
	case status != nil && status.resp.StatusCode == http.StatusUpgradeRequired:
		t.fallback = SendUpgradeRequired

		return nil, errUpgradeRequired
	case errors.Is(err, errClosed):
		return nil, err
	case req.Context().Err() != nil:
		return nil, req.Context().Err()
	case status != nil && status.resp.StatusCode == http.StatusUnauthorized:
		return status.response(req), nil // the caller may renew its credentials
	}
	if t.failures++; t.failures >= webSocketMaxFailures {
		t.fallback = SendWebSocketFailed
	}
	if status != nil {
		return status.response(req), nil
	}

	return nil, err
}

// release frees the transport for the next request.
func (t *WebSocketTransport) release() {
	t.mu.Lock()
	t.inUse = false
	t.mu.Unlock()
}

func (t *WebSocketTransport) report(req *http.Request, s WebSocketSend) {
	if t.Sent != nil {
		t.Sent(req, s)
	}
}

// closeIdle closes the connection when no request used it since.
func (t *WebSocketTransport) closeIdle(c *wsConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inUse || t.conn != c {
		return
	}
	t.conn, t.last, t.lost = nil, nil, SendIdle
	c.close()
}

func or(s, otherwise string) string {
	if s == "" {
		return otherwise
	}

	return s
}

// handshake returns the request's headers for the upgrade and the key of
// the connection they open: its URL and those headers.
func handshake(req *http.Request) (string, http.Header) {
	header := http.Header{}
	for name, values := range req.Header {
		name = http.CanonicalHeaderKey(name)
		switch name {
		case "Accept", "Accept-Encoding", "Content-Type", "Content-Length", "Content-Encoding", "Connection", "Upgrade", headerCodexTurnState:
			continue
		}
		if !strings.HasPrefix(name, "Sec-Websocket-") {
			header[name] = slices.Clone(values)
		}
	}
	header.Add("OpenAI-Beta", webSocketBeta)
	var key strings.Builder
	key.WriteString(webSocketURL(req))
	key.WriteString("\nHost: " + req.Host)
	for _, name := range slices.Sorted(maps.Keys(header)) {
		fmt.Fprintf(&key, "\n%s: %s", name, strings.Join(header[name], ", "))
	}

	return key.String(), header
}

func webSocketURL(req *http.Request) string {
	u := *req.URL
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}

	return u.String()
}

// wsConn is one WebSocket connection and the goroutine that reads it.
type wsConn struct {
	ws      *websocket.Conn
	key     string
	header  http.Header // the handshake response's
	fresh   bool        // no request has used it yet
	written atomic.Int64
	msgs    chan []byte
	done    chan struct{} // closed when reading stops, with err set
	err     error
	quit    chan struct{}
	once    sync.Once
	idle    *time.Timer // closes it unused; set under the transport's mu
}

// handshakeStatus is an upgrade the server answered with another status.
type handshakeStatus struct {
	resp *http.Response
	body []byte
}

func (e *handshakeStatus) Error() string {
	return fmt.Sprintf("the WebSocket upgrade got status %d", e.resp.StatusCode)
}

// response is the status as the HTTP request would have got it.
func (e *handshakeStatus) response(req *http.Request) *http.Response {
	return &http.Response{
		Status: e.resp.Status, StatusCode: e.resp.StatusCode, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: e.resp.Header.Clone(), Body: io.NopCloser(bytes.NewReader(e.body)), ContentLength: int64(len(e.body)), Request: req,
	}
}

func dialWS(req *http.Request, key string, header http.Header, dial func(context.Context, string, string) (net.Conn, error)) (*wsConn, error) {
	if dial == nil {
		dial = (&net.Dialer{Timeout: webSocketConnectTime}).DialContext
	}
	c := &wsConn{key: key, fresh: true, msgs: make(chan []byte, webSocketQueue), done: make(chan struct{}), quit: make(chan struct{})}
	// A context of its own, so the request's httptrace does not see the
	// upgrade's request; the request's end still cancels it.
	ctx, cancel := context.WithTimeout(context.Background(), webSocketConnectTime)
	defer cancel()
	stop := context.AfterFunc(req.Context(), cancel)
	defer stop()
	client := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment, TLSHandshakeTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}

			return countingConn{Conn: conn, n: &c.written}, nil
		},
	}, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse // never takes the credentials elsewhere
	}}
	// An upgrade the server refused leaves its connection idle in the pool;
	// an upgraded one is no longer there.
	defer client.CloseIdleConnections()
	ws, resp, err := websocket.Dial(ctx, webSocketURL(req), &websocket.DialOptions{
		HTTPClient: client, HTTPHeader: header, Host: req.Host, CompressionMode: websocket.CompressionNoContextTakeover,
	})
	switch {
	case err == nil:
	case req.Context().Err() != nil:
		return nil, req.Context().Err()
	case resp != nil && resp.StatusCode != http.StatusSwitchingProtocols:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))

		return nil, &handshakeStatus{resp: resp, body: body}
	default:
		return nil, err
	}
	ws.SetReadLimit(maxSSEFrameBytes)
	c.ws, c.header = ws, resp.Header
	go c.read()

	return c, nil
}

// read queues each message until the connection closes.
func (c *wsConn) read() {
	defer close(c.done)
	for {
		_, data, err := c.ws.Read(context.Background())
		if err != nil {
			c.err = err

			return
		}
		select {
		case c.msgs <- data:
		case <-c.quit:
			c.err = net.ErrClosed

			return
		}
	}
}

func (c *wsConn) close() {
	c.once.Do(func() {
		close(c.quit)
		_ = c.ws.CloseNow()
	})
}

func (c *wsConn) dead() bool {
	select {
	case <-c.done:
		return true
	case <-c.quit:
		return true
	default:
		return false
	}
}

// write sends a message within timeout (0: no limit).
func (c *wsConn) write(ctx context.Context, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	return c.ws.Write(ctx, websocket.MessageText, data)
}

// next returns the next message within timeout (0: no limit).
func (c *wsConn) next(ctx context.Context, timeout time.Duration) ([]byte, error) {
	var expired <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case data := <-c.msgs:
		return data, nil
	case <-c.done:
		select {
		case data := <-c.msgs:
			return data, nil
		default:
			return nil, fmt.Errorf("the WebSocket closed before the response completed: %w", c.err)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-expired:
		return nil, errHeaderTimeout
	}
}

// countingConn counts the bytes written to the network.
type countingConn struct {
	net.Conn

	n *atomic.Int64
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.n.Add(int64(n))

	return n, err // a connection returns its own errors
}

// wsRequest is a request body split for the WebSocket: its fields but
// input and client_metadata, as `,"name":value` each, its input items, and
// its client_metadata, which Codex leaves out of the comparison too.
type wsRequest struct {
	fields   []byte
	input    [][]byte
	metadata []byte
	previous bool // the body sets previous_response_id, so it continues nothing else
}

func parseWSRequest(body []byte) (wsRequest, error) {
	var r wsRequest
	dec := jsontext.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return r, errors.New("the request body is not a JSON object")
	}
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return r, err
		}
		name := tok.String()
		if name == "input" {
			if r.input, err = readItems(dec, body); err != nil {
				return r, err
			}

			continue
		}
		value, err := dec.ReadValue()
		if err != nil {
			return r, err
		}
		end := int(dec.InputOffset())
		if name == "client_metadata" {
			r.metadata = body[end-len(value) : end]

			continue
		}
		r.previous = r.previous || name == "previous_response_id"
		r.fields = append(r.fields, ',')
		if r.fields, err = jsontext.AppendQuote(r.fields, name); err != nil {
			return r, err
		}
		r.fields = append(append(r.fields, ':'), body[end-len(value):end]...)
	}

	return r, nil
}

// readItems reads an array's items as spans of body.
func readItems(dec *jsontext.Decoder, body []byte) ([][]byte, error) {
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '[' {
		return nil, errors.New("the request input is not an array")
	}
	items := [][]byte{}
	for dec.PeekKind() != ']' {
		item, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		end := int(dec.InputOffset())
		items = append(items, body[end-len(item):end])
	}
	_, err := dec.ReadToken()

	return items, err
}

// frame is the request's response.create message: the input after the
// continuation's, with its response's ID, when the request continues it;
// else the whole input and, with a continuation, why not.
func (r wsRequest) frame(cont *continuation, turnState string) ([]byte, bool, string) {
	input, incremental, reason := r.input, false, ""
	if cont != nil && !r.previous {
		var suffix [][]byte
		if suffix, reason = r.after(cont); reason == "" {
			input, incremental = suffix, true
		}
	}
	size := len(r.fields) + 256 + len(turnState)
	for _, item := range input {
		size += len(item) + 1
	}
	frame := make([]byte, 0, size)
	frame = append(frame, `{"type":"response.create"`...)
	frame = append(frame, r.fields...)
	if incremental {
		frame = append(frame, `,"previous_response_id":`...)
		frame, _ = jsontext.AppendQuote(frame, cont.id)
	}
	if metadata := r.clientMetadata(turnState); metadata != nil {
		frame = append(frame, `,"client_metadata":`...)
		frame = append(frame, metadata...)
	}
	frame = append(frame, `,"input":[`...)
	for i, item := range input {
		if i > 0 {
			frame = append(frame, ',')
		}
		frame = append(frame, item...)
	}
	frame = append(frame, "]}"...)

	return frame, incremental, reason
}

// clientMetadata is the request's client_metadata with the turn state,
// which goes there over the WebSocket, as Codex sends it; nil for none.
func (r wsRequest) clientMetadata(turnState string) []byte {
	if turnState == "" {
		return r.metadata
	}
	var fields map[string]jsontext.Value
	if r.metadata != nil && json.Unmarshal(r.metadata, &fields) != nil {
		return r.metadata // not an object: as the body has it
	}
	if fields == nil { // none, or null
		fields = map[string]jsontext.Value{}
	}
	fields[clientMetadataTurnKey], _ = jsontext.AppendQuote(nil, turnState)
	merged, err := json.Marshal(fields, json.Deterministic(true))
	if err != nil {
		return r.metadata
	}

	return merged
}

// after returns the request's input after the continuation's input and
// output, or why the request does not continue it.
func (r wsRequest) after(cont *continuation) ([][]byte, string) {
	if !bytes.Equal(r.fields, cont.fields) {
		return nil, SendRequestChanged
	}
	n := len(cont.input)
	if cont.output == nil || len(r.input) < n+len(cont.output) {
		return nil, SendInputChanged
	}
	for i, item := range cont.input {
		if !bytes.Equal(r.input[i], item) {
			return nil, SendInputChanged
		}
	}
	for i, item := range cont.output {
		if got, ok := readItem(r.input[n+i]); !ok || !got.equal(item) {
			return nil, SendInputChanged
		}
	}

	return r.input[n+len(cont.output):], ""
}

// continuation is a completed response that the next request may continue:
// its request's fields and input, and its output items (nil when one could
// not be named).
type continuation struct {
	id     string
	fields []byte
	input  [][]byte
	output []outputItem
}

// outputItem is what an output item says, which a request that continues
// its response must say in its place: the request may have changed an item
// and kept its ID. The server's item and the request's encoding of it
// differ in other fields and in their order.
type outputItem struct {
	Type             string     `json:"type"`
	ID               string     `json:"id"`
	CallID           string     `json:"call_id"`
	Name             string     `json:"name"`
	Arguments        string     `json:"arguments"`
	Input            string     `json:"input"`
	Phase            string     `json:"phase"`
	EncryptedContent string     `json:"encrypted_content"`
	Content          []itemText `json:"content"`
	Summary          []itemText `json:"summary"`
}

type itemText struct {
	Type    string `json:"type"`
	Text    string `json:"text"`
	Refusal string `json:"refusal"`
}

func (item outputItem) equal(other outputItem) bool {
	return item.Type == other.Type && item.ID == other.ID && item.CallID == other.CallID && item.Name == other.Name &&
		item.Arguments == other.Arguments && item.Input == other.Input && item.Phase == other.Phase &&
		item.EncryptedContent == other.EncryptedContent && slices.Equal(item.Content, other.Content) && slices.Equal(item.Summary, other.Summary)
}

// named reports an item with a type and an ID, which a continuation needs.
func (item outputItem) named() bool { return item.Type != "" && item.ID != "" }

func readItem(data []byte) (outputItem, bool) {
	var item outputItem
	if json.Unmarshal(data, &item) != nil || !item.named() {
		return outputItem{}, false
	}

	return item, true
}

// wsBody is a response's events as an SSE stream.
type wsBody struct {
	t   *WebSocketTransport
	c   *wsConn
	ctx context.Context
	r   wsRequest

	// Read's: the frames not yet read and the response so far.
	out      bytes.Buffer
	id       string
	output   map[int]outputItem
	unnamed  bool // an output item without a type and ID
	listed   []outputItem
	terminal string
	err      error

	once sync.Once
}

func (b *wsBody) Read(p []byte) (int, error) {
	for b.out.Len() == 0 {
		switch {
		case b.terminal != "":
			return 0, io.EOF
		case b.err != nil:
			return 0, b.err
		}
		data, err := b.c.next(b.ctx, 0)
		if err != nil {
			b.err = err
			b.finish(nil, false, b.ctx.Err() == nil, SendInterrupted)

			continue
		}
		b.event(data)
	}

	return b.out.Read(p)
}

// Close abandons a response that has not ended, which may be being read:
// the connection closes, and the Read returns.
func (b *wsBody) Close() error {
	b.finish(nil, false, b.ctx.Err() == nil, SendInterrupted)

	return nil
}

// event passes one event on as an SSE frame and keeps what a continuation
// needs from it.
func (b *wsBody) event(data []byte) {
	var event struct {
		Type     string `json:"type"`
		Response *struct {
			ID     string       `json:"id"`
			Output []outputItem `json:"output"`
		} `json:"response"`
		OutputIndex *int        `json:"output_index"`
		Item        *outputItem `json:"item"`
	}
	_ = json.Unmarshal(data, &event)
	switch event.Type {
	case "response.output_item.done":
		if event.OutputIndex == nil || event.Item == nil || !event.Item.named() {
			b.unnamed = true

			break
		}
		if b.output == nil {
			b.output = map[int]outputItem{}
		}
		b.output[*event.OutputIndex] = *event.Item
	case "response.completed", "response.failed", "response.incomplete", "error":
		b.terminal = event.Type
		if event.Response != nil {
			b.id, b.listed = event.Response.ID, event.Response.Output
		}
	}
	if bytes.ContainsAny(data, "\r\n") { // an SSE data line has none
		if compact := jsontext.Value(slices.Clone(data)); compact.Compact() == nil {
			data = compact
		}
	}
	b.out.WriteString("data: ")
	b.out.Write(data)
	b.out.WriteString("\n\n")
	if b.terminal == "" {
		return
	}
	var cont *continuation
	if b.terminal == "response.completed" && b.id != "" {
		cont = &continuation{id: b.id, fields: b.r.fields, input: b.r.input, output: b.outputKeys()}
	}
	// Codex drops a connection that answered with an error.
	b.finish(cont, b.terminal != "error", false, SendNotCompleted)
}

// outputKeys are the response's output items in order: those the
// completed response lists, else those its events finished; nil when one
// cannot be named.
func (b *wsBody) outputKeys() []outputItem {
	if len(b.listed) > 0 {
		for _, item := range b.listed {
			if !item.named() {
				return nil
			}
		}

		return b.listed
	}
	if b.unnamed {
		return nil
	}
	items := make([]outputItem, 0, len(b.output))
	for i := range len(b.output) {
		item, ok := b.output[i]
		if !ok {
			return nil
		}
		items = append(items, item)
	}

	return items
}

// finish frees the transport, once: cont, a completed response, is the
// next request's continuation; without one, reason says why there is
// none. A connection not kept closes, and failed counts it toward falling
// back to HTTP.
func (b *wsBody) finish(cont *continuation, keep, failed bool, reason string) {
	b.once.Do(func() {
		t := b.t
		if !keep {
			b.c.close()
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		t.inUse = false
		if t.conn != b.c {
			return // closed meanwhile
		}
		t.last = cont
		if cont == nil {
			t.lost = reason
		}
		if !keep {
			t.conn = nil
			if failed {
				if t.failures++; t.failures >= webSocketMaxFailures {
					t.fallback = SendWebSocketFailed
				}
			}

			return
		}
		t.failures = 0
		c := b.c
		c.idle = time.AfterFunc(webSocketIdleTimeout, func() { t.closeIdle(c) })
	})
}

// leading reports an event the ChatGPT backend sends before a response's
// own, in place of HTTP's response headers: codex.rate_limits, and
// codex.response.metadata, whose headers, such as x-codex-turn-state, it
// adds to h.
func leading(data []byte, h http.Header) bool {
	var event struct {
		Type    string                    `json:"type"`
		Headers map[string]jsontext.Value `json:"headers"`
	}
	if json.Unmarshal(data, &event) != nil || !strings.HasPrefix(event.Type, "codex.") {
		return false
	}
	if event.Type == "codex.response.metadata" {
		for name, value := range event.Headers {
			var s string
			if json.Unmarshal(value, &s) == nil {
				h.Set(name, s)
			}
		}
	}

	return true
}

// wsError is an error event. An upgraded connection reports a request
// that failed so, with the HTTP status the request would have got.
type wsError struct {
	Status  int
	Error   jsontext.Value
	Headers map[string]jsontext.Value
}

func errorEvent(data []byte) *wsError {
	var event struct {
		Type       string                    `json:"type"`
		Status     int                       `json:"status"`
		StatusCode int                       `json:"status_code"`
		Error      jsontext.Value            `json:"error"`
		Headers    map[string]jsontext.Value `json:"headers"`
	}
	if json.Unmarshal(data, &event) != nil || event.Type != "error" {
		return nil
	}
	// Codex reads headers, such as Retry-After, from the error too.
	var nested struct {
		Headers map[string]jsontext.Value `json:"headers"`
	}
	if json.Unmarshal(event.Error, &nested) == nil && len(nested.Headers) > 0 {
		maps.Copy(nested.Headers, event.Headers)
		event.Headers = nested.Headers
	}

	return &wsError{Status: max(event.Status, event.StatusCode), Error: event.Error, Headers: event.Headers}
}

func (e *wsError) code() string {
	if e == nil {
		return ""
	}
	var body struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(e.Error, &body)

	return body.Code
}

// response is the error as the HTTP request would have got it.
func (e *wsError) response(req *http.Request) *http.Response {
	h := http.Header{"Content-Type": {"application/json"}}
	for name, value := range e.Headers {
		var s string
		if json.Unmarshal(value, &s) != nil {
			s = string(value)
		}
		h.Set(name, s)
	}
	detail := e.Error
	if len(detail) == 0 {
		detail = jsontext.Value(`{}`)
	}
	body := slices.Concat([]byte(`{"error":`), detail, []byte(`}`))

	return &http.Response{
		Status: fmt.Sprintf("%d %s", e.Status, http.StatusText(e.Status)), StatusCode: e.Status, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: h, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: req,
	}
}

// withBody is the request with its body again, to send over HTTP.
func withBody(req *http.Request, body []byte) *http.Request {
	clone := req.Clone(req.Context())
	clone.Body, clone.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
	clone.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }

	return clone
}
