package localfile

import (
	"encoding/json/jsontext"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viktordanov/uah-core/harness/inbox"
	"github.com/viktordanov/uah-core/harness/llm"
	"github.com/viktordanov/uah-core/harness/operation"
	"github.com/viktordanov/uah-core/harness/session"
	"github.com/viktordanov/uah-core/harness/sessionstore"
	"github.com/viktordanov/uah-core/harness/tool"
)

// BenchmarkResumeRunStart measures what a run does with a resumed session
// before its first model request: resume it, page through its history as the
// coordinator restores it, and append the run's first input.
func BenchmarkResumeRunStart(b *testing.B) {
	id := session.ID("session-1")
	type benchSession struct {
		name    string
		encoded []byte
	}
	var sessions []benchSession
	for _, rounds := range []int{100, 1000} {
		state := testState(b, id, rounds)
		encoded, err := encodeInitialLog(state.Snapshot.Session, state.Items)
		if err != nil {
			b.Fatal(err)
		}
		sessions = append(sessions, benchSession{fmt.Sprintf("rounds=%d", rounds), encoded})
	}
	sessions = append(sessions, benchSession{"runs=90", longSession(b, id, 90)})
	for _, s := range sessions {
		directory := b.TempDir()
		path := filepath.Join(directory, sessionFilename(id))
		encoded := s.encoded
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				if err := os.WriteFile(path, encoded, 0o600); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				store, err := New(directory)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := store.Resume(b.Context(), id); err != nil {
					b.Fatal(err)
				}
				after := sessionstore.BeforeFirst
				for {
					page, err := store.Items(b.Context(), id, after, 256)
					if err != nil {
						b.Fatal(err)
					}
					if !page.More {
						break
					}
					after = page.NextAfter
				}
				if err := store.AppendInput(b.Context(), id, inbox.Input{
					ID: inbox.ID("input-next"), Kind: inbox.InputExternal, Payload: jsontext.Value(`"next"`),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// longSession is the file of a long coding session: runs of a user message
// and 44 turns, all but the last a command whose output, 13 kB, is in its
// operation's final state. At 90 runs it holds about 12,000 items in 80 MB.
func longSession(tb testing.TB, id session.ID, runs int) []byte {
	tb.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	line := func(kind recordType, value any) []byte {
		encoded, err := encodeRecord(kind, value)
		if err != nil {
			tb.Fatal(err)
		}
		return encoded
	}
	text := strings.Repeat("Some text a person or a model wrote about the code. ", 20)
	reasoning := jsontext.Value(fmt.Sprintf(`{"type":"reasoning","encrypted_content":%q}`, strings.Repeat("gAAAAABo", 400)))
	arguments := fmt.Sprintf(`{"command":%q}`, strings.Repeat("go test ./... ", 30))
	output := jsontext.Value(fmt.Sprintf(`{"exit_code":0,"output":%q}`, strings.Repeat("--- PASS: TestSomething (0.01s)\n", 420)))
	encoded := line(recordSession, sessionRecord{Version: formatVersion, Session: session.Session{ID: id, CreatedAt: at}})
	var sequence sessionstore.Sequence
	item := func(kind sessionstore.ItemKind, data any, operations []operation.Operation) {
		sequence++
		encoded = append(encoded, line(recordItem, itemRecord{
			Item:       sessionstore.Item{Sequence: sequence, RecordedAt: at, Kind: kind, Data: data},
			Operations: operations,
		})...)
	}
	previous := session.TurnID("")
	for run := range runs {
		item(sessionstore.ItemInput, inbox.Input{
			ID: inbox.ID(fmt.Sprintf("input-%d", run)), Kind: inbox.InputExternal, Payload: jsontext.Value(fmt.Sprintf("%q", text)),
		}, nil)
		for step := range 44 {
			turn := session.TurnID(fmt.Sprintf("turn-%d-%d", run, step))
			item(sessionstore.ItemTurn, session.Turn{ID: turn, PreviousTurnID: previous, Type: session.TurnRegular}, nil)
			previous = turn
			response := llm.Response{ID: "response-" + string(turn), Output: []llm.Item{
				{Type: llm.ItemReasoning, Data: llm.Reasoning{Summary: []string{text}, Raw: reasoning}},
			}}
			if step == 43 {
				response.Output = append(response.Output, llm.Item{Type: llm.ItemMessage, Data: llm.Message{Role: llm.RoleAssistant, Text: text}})
				item(sessionstore.ItemModelResponse, sessionstore.ModelResponse{TurnID: turn, Response: response}, nil)
				continue
			}
			call := "call-" + string(turn)
			response.Output = append(response.Output, llm.Item{Type: llm.ItemToolCall, Data: llm.ToolCall{CallID: call, Name: "Bash", Arguments: arguments}})
			item(sessionstore.ItemModelResponse, sessionstore.ModelResponse{TurnID: turn, Response: response}, nil)
			op := operation.Operation{
				ID: operation.ID("operation-" + string(turn)), Type: "shell", Version: 1,
				Status: operation.StatusReady, State: jsontext.Value(arguments),
			}
			item(sessionstore.ItemToolCallStatus, sessionstore.ToolCallStatus{
				TurnID: turn, CallID: call, Status: tool.CallStatus{WaitingFor: []operation.ID{op.ID}},
			}, []operation.Operation{op})
			op.Status, op.State = operation.StatusAwaiting, jsontext.Value(`{"process_group":4242}`)
			encoded = append(encoded, line(recordOperation, operationRecord{Operation: op})...)
			op.Status, op.State = operation.StatusCompleted, output
			encoded = append(encoded, line(recordOperation, operationRecord{Operation: op})...)
		}
	}
	return encoded
}

// testState is a session of rounds of a user message, a turn, and a model
// response.
func testState(tb testing.TB, id session.ID, rounds int) storedState {
	tb.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	text := strings.Repeat("Some output from a tool, quoted back by the model. ", 40)
	state := newStoredState(id, at)
	previous := session.TurnID("")
	for round := range rounds {
		turn := session.TurnID(fmt.Sprintf("turn-%d", round))
		if err := state.appendInput(inbox.Input{
			ID: inbox.ID(fmt.Sprintf("input-%d", round)), Kind: inbox.InputExternal, Payload: jsontext.Value(fmt.Sprintf("%q", text)),
		}, at); err != nil {
			tb.Fatal(err)
		}
		if err := state.appendTurn(session.Turn{ID: turn, PreviousTurnID: previous, Type: session.TurnRegular}, at); err != nil {
			tb.Fatal(err)
		}
		if err := state.appendModelResponse(sessionstore.ModelResponse{TurnID: turn, Response: llm.Response{
			ID:     fmt.Sprintf("response-%d", round),
			Output: []llm.Item{{Type: llm.ItemMessage, Data: llm.Message{Role: llm.RoleAssistant, Text: text}}},
		}}, at); err != nil {
			tb.Fatal(err)
		}
		previous = turn
	}
	return state
}

func TestResumedSessionReadsTheFileOnce(t *testing.T) {
	directory := t.TempDir()
	writer, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	id := session.ID("session-1")
	if err := writer.publishInitialState(testState(t, id, 5)); err != nil {
		t.Fatal(err)
	}
	want := readAllItems(t, writer, id)

	store, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	path := store.sessionPath(id)
	if err := os.Chmod(path, 0o200); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(path, 0o600); err != nil {
			t.Error(err)
		}
	})
	if _, err := os.ReadFile(path); err == nil {
		t.Skip("platform permits reading a write-only file")
	}

	if got := readAllItems(t, store, id); !reflect.DeepEqual(got, want) {
		t.Fatalf("items = %#v\nwant %#v", got, want)
	}
	// The history was read to its end, so the store let it go: a later read
	// is not the restore's.
	if _, err := store.Items(t.Context(), id, sessionstore.BeforeFirst, 1); err == nil {
		t.Fatal("a second read of the history did not read the session file")
	}
	if err := store.AppendTurn(t.Context(), id, session.Turn{
		ID: "turn-next", PreviousTurnID: "turn-4", Type: session.TurnRegular,
	}); err != nil {
		t.Fatal(err)
	}
	// The write changed the file, so the history is read from it again.
	if _, err := store.Items(t.Context(), id, sessionstore.BeforeFirst, 1); err == nil {
		t.Fatal("items after a write did not read the session file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	got := readAllItems(t, store, id)
	if len(got) != len(want)+1 || got[len(got)-1].Sequence != want[len(want)-1].Sequence+1 {
		t.Fatalf("%d items after the write, want %d", len(got), len(want)+1)
	}
}

func TestResumedSessionFollowsOtherWriters(t *testing.T) {
	directory := t.TempDir()
	other, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	id := session.ID("session-1")
	if err := other.publishInitialState(testState(t, id, 2)); err != nil {
		t.Fatal(err)
	}
	store, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	input := inbox.Input{ID: "input-other", Kind: inbox.InputExternal, Payload: jsontext.Value(`"other"`)}
	if err := other.AppendInput(t.Context(), id, input); err != nil {
		t.Fatal(err)
	}

	items := readAllItems(t, store, id)
	if len(items) != 7 || items[6].Data.(inbox.Input).ID != input.ID {
		t.Fatalf("%d items, want 7 with the other writer's input last", len(items))
	}
	if err := store.AppendInput(t.Context(), id, inbox.Input{
		ID: "input-next", Kind: inbox.InputExternal, Payload: jsontext.Value(`"next"`),
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	items = readAllItems(t, reopened, id)
	if len(items) != 8 || items[6].Data.(inbox.Input).ID != input.ID || items[7].Sequence != 8 {
		t.Fatalf("%d items, want 8 with both writers' inputs", len(items))
	}
}

func readAllItems(t *testing.T, store *Store, id session.ID) []sessionstore.Item {
	t.Helper()
	var items []sessionstore.Item
	after := sessionstore.BeforeFirst
	for {
		page, err := store.Items(t.Context(), id, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, page.Items...)
		if !page.More {
			return items
		}
		after = page.NextAfter
	}
}
