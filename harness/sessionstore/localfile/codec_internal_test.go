package localfile

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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

func TestEncodeInitialLogRejectsInvalidState(t *testing.T) {
	tests := []struct {
		name  string
		state storedState
		want  string
	}{
		{
			name:  "invalid session ID",
			state: newStoredState("invalid/session", stateCreatedAt),
			want:  "must contain only ASCII letters, digits, and dashes",
		},
		{
			name: "unencodable creation time",
			state: newStoredState(
				"session-1",
				time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC),
			),
			want: "encode session",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeInitialLog(test.state.Snapshot.Session, test.state.Items); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestEncodeInitialLogRejectsInvalidItems(t *testing.T) {
	tests := []struct {
		name string
		item sessionstore.Item
		want string
	}{
		{
			name: "non-contiguous sequence",
			item: sessionstore.Item{
				Sequence: 2, RecordedAt: stateUpdatedAt,
				Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
			},
			want: "non-contiguous sequence",
		},
		{
			name: "zero recorded time",
			item: sessionstore.Item{
				Sequence: 1, Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
			},
			want: "recorded time is zero",
		},
		{
			name: "wrong data type",
			item: sessionstore.Item{
				Sequence: 1, RecordedAt: stateUpdatedAt,
				Kind: sessionstore.ItemTurn, Data: inbox.Input{},
			},
			want: "turn data must be session.Turn",
		},
		{
			name: "invalid model response",
			item: sessionstore.Item{
				Sequence: 1, RecordedAt: stateUpdatedAt,
				Kind: sessionstore.ItemModelResponse,
				Data: sessionstore.ModelResponse{
					TurnID:   "turn-1",
					Response: llm.Response{Usage: llm.Usage{Raw: jsontext.Value(`{`)}},
				},
			},
			want: "unexpected EOF",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newStoredState("session-1", stateCreatedAt)
			state.Items = []sessionstore.Item{test.item}
			if _, err := encodeInitialLog(state.Snapshot.Session, state.Items); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestEncodeInitialLogEmbedsJSONValues(t *testing.T) {
	state := newStoredState("session-1", stateCreatedAt)
	if err := state.appendInput(inbox.Input{
		ID: "input-1", Kind: inbox.InputExternal,
		Payload: jsontext.Value(`{"input":true}`),
	}, stateUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if err := state.appendTurn(session.Turn{ID: "turn-1", Type: session.TurnRegular}, stateUpdatedAt); err != nil {
		t.Fatal(err)
	}
	if err := state.appendModelResponse(sessionstore.ModelResponse{
		TurnID: "turn-1",
		Response: llm.Response{
			Output: []llm.Item{{
				Type: llm.ItemReasoning,
				Data: llm.Reasoning{Raw: jsontext.Value(`{"reasoning":true}`)},
			}},
			Usage: llm.Usage{Raw: jsontext.Value(`{"usage":true}`)},
		},
	}, stateUpdatedAt); err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeInitialLog(state.Snapshot.Session, state.Items)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"Payload":{"input":true}`,
		`"Raw":{"reasoning":true}`,
		`"Raw":{"usage":true}`,
	} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("encoded state does not contain %s: %s", want, encoded)
		}
	}
}

func TestEncodeRecordKeepsEmbeddedJSONOnOneLine(t *testing.T) {
	encoded, err := encodeRecord(recordOperation, jsontext.Value("{\n  \"step\": 1\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(encoded, []byte{'\n'}) != 1 || encoded[len(encoded)-1] != '\n' {
		t.Fatalf("encoded record is not one JSONL line: %q", encoded)
	}
}

func TestDecodeLogFoldsOperationUpdates(t *testing.T) {
	encoded := emptyLog(t)
	turn := sessionstore.Item{
		Sequence: 1, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
	}
	encoded = append(encoded, mustRecord(t, recordItem, itemRecord{Item: turn})...)
	initial := validOperation("operation-1", operation.StatusReady)
	status := sessionstore.Item{
		Sequence: 2, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemToolCallStatus,
		Data: sessionstore.ToolCallStatus{
			TurnID: "turn-1", CallID: "call-1",
			Status: tool.CallStatus{WaitingFor: []operation.ID{"operation-1"}},
		},
	}
	encoded = append(encoded, mustRecord(t, recordItem, itemRecord{
		Item: status, Operations: []operation.Operation{initial},
	})...)
	updated := initial
	updated.Status = operation.StatusAwaiting
	updated.State = jsontext.Value(`{"step":2}`)
	encoded = append(encoded, mustRecord(t, recordOperation, operationRecord{Operation: updated})...)

	state, committedSize, err := decodeLog(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if committedSize != int64(len(encoded)) {
		t.Fatalf("committed size = %d, want %d", committedSize, len(encoded))
	}
	if len(state.Operations) != 1 || state.Operations[0].Status != operation.StatusAwaiting ||
		string(state.Operations[0].State) != `{"step":2}` {
		t.Fatalf("operations = %#v", state.Operations)
	}
}

func TestDecodeLogIgnoresIncompleteTail(t *testing.T) {
	committed := emptyLog(t)
	encoded := append(append([]byte(nil), committed...), []byte(`{"type":"item"`)...)
	state, committedSize, err := decodeLog(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot.Session.ID != "session-1" || committedSize != int64(len(committed)) {
		t.Fatalf("state = %#v, committed size = %d", state, committedSize)
	}

	completedInvalid := append(append([]byte(nil), committed...), []byte("{\n")...)
	if _, _, err := decodeLog(completedInvalid); err == nil {
		t.Fatal("invalid completed record decoded")
	}
}

func TestDecodeLogPreservesInheritedInput(t *testing.T) {
	encoded := emptyLog(t)
	encoded = append(encoded, mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
		Sequence: 1, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "parent-turn", Type: session.TurnRegular},
	}})...)
	encoded = append(encoded, mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
		Sequence: 2, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemInput,
		Data: inbox.Input{
			ID: "parent-input", Kind: inbox.InputExternal, Payload: jsontext.Value(`{}`),
		},
	}})...)
	encoded = append(encoded, mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
		Sequence: 3, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemFork,
		Data: sessionstore.Fork{ParentID: "parent", PreviousTurnID: "parent-turn"},
	}})...)

	state, _, err := decodeLog(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Items[1].Data.(inbox.Input).ID; got != "parent-input" {
		t.Fatalf("inherited input ID = %q, want parent-input", got)
	}
}

func TestDecodeLogRejectsInvalidRecords(t *testing.T) {
	header := emptyLog(t)
	turn := sessionstore.Item{
		Sequence: 1, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
	}
	status := sessionstore.Item{
		Sequence: 2, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemToolCallStatus,
		Data: sessionstore.ToolCallStatus{
			TurnID: "turn-1", CallID: "call-1",
			Status: tool.CallStatus{WaitingFor: []operation.ID{"operation-1"}},
		},
	}
	response := sessionstore.Item{
		Sequence: 2, RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemModelResponse,
		Data: sessionstore.ModelResponse{
			TurnID: "turn-1", Response: llm.Response{Output: []llm.Item{}},
		},
	}
	withTurn := append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{Item: turn})...)
	withResponse := append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{Item: response})...)
	initial := validOperation("operation-1", operation.StatusReady)
	withStatus := append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{
		Item: status, Operations: []operation.Operation{initial},
	})...)

	tests := []struct {
		name    string
		encoded []byte
		want    string
	}{
		{name: "no committed record", encoded: []byte(`{}`), want: "no committed records"},
		{name: "unsupported record", encoded: mustRecord(t, "unknown", struct{}{}), want: "unsupported record type"},
		{name: "item first", encoded: mustRecord(t, recordItem, itemRecord{Item: turn}), want: "session record must be first"},
		{
			name: "operation first",
			encoded: mustRecord(t, recordOperation, operationRecord{
				Operation: initial,
			}),
			want: "session record must be first",
		},
		{
			name: "session not first",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordSession, sessionRecord{
				Version: formatVersion, Session: session.Session{ID: "other", CreatedAt: stateCreatedAt},
			})...),
			want: "session record must be first",
		},
		{
			name:    "malformed session data",
			encoded: mustRecord(t, recordSession, jsontext.Value(`[]`)),
			want:    "decode session record",
		},
		{
			name: "legacy version",
			encoded: mustRecord(t, recordSession, sessionRecord{
				Version: 1, Session: session.Session{ID: "session-1", CreatedAt: stateCreatedAt},
			}),
			want: "legacy session format version 1 cannot be resumed",
		},
		{
			name: "unsupported version",
			encoded: mustRecord(t, recordSession, sessionRecord{
				Version: formatVersion + 1, Session: session.Session{ID: "session-1", CreatedAt: stateCreatedAt},
			}),
			want: "unsupported session format version",
		},
		{
			name: "invalid session ID",
			encoded: mustRecord(t, recordSession, sessionRecord{
				Version: formatVersion,
				Session: session.Session{ID: "invalid/id", CreatedAt: stateCreatedAt},
			}),
			want: "must contain only ASCII letters, digits, and dashes",
		},
		{
			name: "zero creation time",
			encoded: mustRecord(t, recordSession, sessionRecord{
				Version: formatVersion, Session: session.Session{ID: "session-1"},
			}),
			want: "creation time is zero",
		},
		{
			name:    "malformed item data",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, jsontext.Value(`[]`))...),
			want:    "decode item record",
		},
		{
			name: "non-contiguous item",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
				Sequence: 2, RecordedAt: stateUpdatedAt,
				Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
			}})...),
			want: "has sequence 2, want 1",
		},
		{
			name: "zero item time",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
				Sequence: 1, Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
			}})...),
			want: "recorded time is zero",
		},
		{
			name: "empty turn ID",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 1, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemTurn, Data: session.Turn{},
				},
			})...),
			want: "turn ID is empty",
		},
		{
			name: "wrong previous turn",
			encoded: append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 2, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemTurn,
					Data: session.Turn{ID: "turn-2", PreviousTurnID: "other", Type: session.TurnRegular},
				},
			})...),
			want: `previous turn is "other", want "turn-1"`,
		},
		{
			name: "duplicate turn",
			encoded: append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 2, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemTurn,
					Data: session.Turn{ID: "turn-1", PreviousTurnID: "turn-1", Type: session.TurnRegular},
				},
			})...),
			want: `append turn "turn-1": file already exists`,
		},
		{
			name: "response for missing turn",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 1, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemModelResponse,
					Data: sessionstore.ModelResponse{
						TurnID: "missing", Response: llm.Response{Output: []llm.Item{}},
					},
				},
			})...),
			want: "file does not exist",
		},
		{
			name: "duplicate response",
			encoded: append(append([]byte(nil), withResponse...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 3, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemModelResponse,
					Data: sessionstore.ModelResponse{
						TurnID: "turn-1", Response: llm.Response{Output: []llm.Item{}},
					},
				},
			})...),
			want: `append model response for turn "turn-1": file already exists`,
		},
		{
			name: "status for missing turn",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 1, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemToolCallStatus,
					Data: sessionstore.ToolCallStatus{
						TurnID: "missing", CallID: "call-1",
						Status: tool.CallStatus{WaitingFor: []operation.ID{"operation-1"}},
					},
				},
				Operations: []operation.Operation{initial},
			})...),
			want: "file does not exist",
		},
		{
			name: "operations on turn",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{
				Item: turn, Operations: []operation.Operation{validOperation("operation-1", operation.StatusReady)},
			})...),
			want: "initializes operations",
		},
		{
			name:    "status without operation",
			encoded: append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{Item: status})...),
			want:    "must initialize at least one operation",
		},
		{
			name: "invalid initialized operation",
			encoded: append(append([]byte(nil), withTurn...), mustRecord(t, recordItem, itemRecord{
				Item: status,
				Operations: []operation.Operation{{
					Type: "test", Version: 1, Status: operation.StatusReady,
				}},
			})...),
			want: "operation ID is empty",
		},
		{
			name: "duplicate initialized operation",
			encoded: append(append([]byte(nil), withStatus...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 3, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemToolCallStatus,
					Data: sessionstore.ToolCallStatus{
						TurnID: "turn-1", CallID: "call-2",
						Status: tool.CallStatus{WaitingFor: []operation.ID{"operation-1"}},
					},
				},
				Operations: []operation.Operation{initial},
			})...),
			want: `initialize operation "operation-1": file already exists`,
		},
		{
			name: "operation inherited across fork",
			encoded: append(append([]byte(nil), withStatus...), mustRecord(t, recordItem, itemRecord{
				Item: sessionstore.Item{
					Sequence: 3, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemFork,
					Data: sessionstore.Fork{ParentID: "parent", PreviousTurnID: "turn-1"},
				},
			})...),
			want: "inherited item 1 initializes operations",
		},
		{
			name: "operation update inside inherited prefix",
			encoded: append(
				append(
					append(append([]byte(nil), header...), mustRecord(t, recordItem, itemRecord{Item: turn})...),
					mustRecord(t, recordOperation, operationRecord{Operation: initial})...,
				),
				mustRecord(t, recordItem, itemRecord{Item: sessionstore.Item{
					Sequence: 2, RecordedAt: stateUpdatedAt,
					Kind: sessionstore.ItemFork,
					Data: sessionstore.Fork{ParentID: "parent", PreviousTurnID: "turn-1"},
				}})...,
			),
			want: "appears in inherited history",
		},
		{
			name:    "malformed operation update",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordOperation, jsontext.Value(`[]`))...),
			want:    "decode operation record",
		},
		{
			name: "missing operation update",
			encoded: append(append([]byte(nil), header...), mustRecord(t, recordOperation, operationRecord{
				Operation: validOperation("missing", operation.StatusReady),
			})...),
			want: "file does not exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := decodeLog(test.encoded); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTurnTypesSurviveReopen(t *testing.T) {
	for _, turnType := range []session.TurnType{session.TurnRegular, session.TurnCompaction} {
		for _, outcome := range []string{"incomplete", "response"} {
			t.Run(string(turnType)+"/"+outcome, func(t *testing.T) {
				state := newStoredState("session-1", stateCreatedAt)
				if err := state.appendTurn(session.Turn{ID: "turn-1", Type: turnType}, stateUpdatedAt); err != nil {
					t.Fatal(err)
				}
				response := validResponse("turn-1")
				if outcome == "response" {
					if err := state.appendModelResponse(response, stateUpdatedAt); err != nil {
						t.Fatal(err)
					}
				}
				encoded, err := encodeInitialLog(state.Snapshot.Session, state.Items)
				if err != nil {
					t.Fatal(err)
				}
				reopened, _, err := decodeLog(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(state, reopened) {
					t.Fatal("reopen changed state")
				}
				err = reopened.appendModelResponse(response, stateUpdatedAt)
				if outcome == "response" {
					if !errors.Is(err, fs.ErrExist) {
						t.Fatalf("duplicate response accepted: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func emptyLog(t *testing.T) []byte {
	t.Helper()
	return mustRecord(t, recordSession, sessionRecord{
		Version: formatVersion,
		Session: session.Session{ID: "session-1", CreatedAt: stateCreatedAt},
	})
}

func mustRecord(t *testing.T, kind recordType, value any) []byte {
	t.Helper()
	encoded, err := encodeRecord(kind, value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestDecodeLogMatchesDecodingByType compares decodeLog with the decode it
// replaced, which decoded each line's envelope and then its data by the
// record's type: the same state, committed size, and error for each log.
func TestDecodeLogMatchesDecodingByType(t *testing.T) {
	for name, encoded := range decodeCases(t) {
		t.Run(name, func(t *testing.T) { assertDecodesByType(t, encoded) })
	}
}

func FuzzDecodeLog(f *testing.F) {
	for _, encoded := range decodeCases(f) {
		f.Add(encoded)
	}
	f.Fuzz(assertDecodesByType)
}

// TestDecodeLogDecodesEachLineOnce checks that decoding a session, most of
// it commands' output, allocates less than the decode by type, which copied
// each record's data to decode it a second time.
func TestDecodeLogDecodesEachLineOnce(t *testing.T) {
	encoded := longSession(t, "session-1", 4)
	allocated := func(decode func([]byte) (storedState, int64, error)) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if _, _, err := decode(encoded); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	if got, byType := allocated(decodeLog), allocated(decodeLogByType); got > byType*3/4 {
		t.Fatalf("decoding %d bytes allocated %d, decoding by type %d", len(encoded), got, byType)
	}
}

func assertDecodesByType(t *testing.T, encoded []byte) {
	state, size, err := decodeLog(encoded)
	wantState, wantSize, wantErr := decodeLogByType(encoded)
	if fmt.Sprint(err) != fmt.Sprint(wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if size != wantSize || !reflect.DeepEqual(state, wantState) {
		t.Fatalf("decoded %d bytes to %#v\nwant %d bytes, %#v", size, state, wantSize, wantState)
	}
}

// decodeCases are logs to decode: valid ones, a fork's, and lines that only
// the decode by type accepts or that neither accepts.
func decodeCases(tb testing.TB) map[string][]byte {
	tb.Helper()
	line := func(kind recordType, value any) []byte {
		encoded, err := encodeRecord(kind, value)
		if err != nil {
			tb.Fatal(err)
		}
		return encoded
	}
	long := longSession(tb, "session-1", 1)
	child, err := forkStoredState(testState(tb, "parent", 3), "session-1", "turn-1", stateUpdatedAt)
	if err != nil {
		tb.Fatal(err)
	}
	fork, err := encodeInitialLog(child.Snapshot.Session, child.Items)
	if err != nil {
		tb.Fatal(err)
	}
	fork = append(fork, line(recordItem, itemRecord{Item: sessionstore.Item{
		Sequence: sessionstore.Sequence(len(child.Items) + 1), RecordedAt: stateUpdatedAt,
		Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-own", PreviousTurnID: "turn-1", Type: session.TurnRegular},
	}})...)
	header := line(recordSession, sessionRecord{Version: formatVersion, Session: session.Session{ID: "session-1", CreatedAt: stateCreatedAt}})
	turn := line(recordItem, itemRecord{Item: sessionstore.Item{
		Sequence: 1, RecordedAt: stateUpdatedAt, Kind: sessionstore.ItemTurn, Data: session.Turn{ID: "turn-1", Type: session.TurnRegular},
	}})
	ready := line(recordOperation, operationRecord{Operation: validOperation("operation-1", operation.StatusReady)})
	join := func(lines ...[]byte) []byte { return bytes.Join(lines, nil) }
	// with puts member, a field of another record type's data, in the line's data.
	with := func(line []byte, member string) []byte {
		return bytes.Replace(line, []byte(`"data":{`), []byte(`"data":{`+member+`,`), 1)
	}
	cases := map[string][]byte{
		"long session":           long,
		"incomplete tail":        append(append([]byte(nil), long[:len(long)-1]...), `{"type":"item"`...),
		"fork":                   fork,
		"fields of other types":  join(with(header, `"Operation":5`), with(turn, `"Version":"two"`)),
		"operation with an item": join(header, turn, with(ready, `"Item":[]`)),
		"not a record":           join(header, []byte("{\n")),
		"duplicate type":         join(header, []byte(`{"type":"item","type":"item","data":{}}`+"\n")),
		"unknown type":           join(header, []byte(`{"type":"unknown","data":5}`+"\n")),
		"malformed session data": []byte(`{"type":"session","data":[]}` + "\n"),
		"null item data":         join(header, []byte(`{"type":"item","data":{"Item":{"Kind":"turn","Data":null}}}`+"\n")),
		"session not first":      join(header, header),
		"operation first":        ready,
		"unknown operation":      join(header, ready),
		"session without data":   []byte(`{"type":"session"}` + "\n"),
		"item with null data":    join(header, []byte(`{"type":"item","data":null}`+"\n")),
	}
	for _, name := range []string{"golden-session", "golden-fork", "legacy-session", "soft-stop-session"} {
		encoded, err := os.ReadFile(filepath.Join("testdata", name+sessionFileSuffix))
		if err != nil {
			tb.Fatal(err)
		}
		cases[name] = encoded
	}
	return cases
}

// decodeLogByType is decodeLog as it was before it decoded each line in one
// pass, for the comparison.
func decodeLogByType(encoded []byte) (storedState, int64, error) {
	lastNewline := bytes.LastIndexByte(encoded, '\n')
	if lastNewline < 0 {
		return storedState{}, 0, fmt.Errorf("session log has no committed records")
	}
	committedSize := int64(lastNewline + 1)
	lines := bytes.Split(encoded[:committedSize], []byte{'\n'})
	lines = lines[:len(lines)-1]

	var value storedState
	records := make([]decodedRecord, 0, len(lines))
	itemCount := 0
	inheritedItemCount := 0
	for lineIndex, line := range lines {
		var record logRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return storedState{}, 0, fmt.Errorf("decode record %d: %w", lineIndex+1, err)
		}
		switch record.Type {
		case recordSession:
			if lineIndex != 0 {
				return storedState{}, 0, fmt.Errorf("session record must be first")
			}
			var header sessionRecord
			if err := json.Unmarshal(record.Data, &header); err != nil {
				return storedState{}, 0, fmt.Errorf("decode session record: %w", err)
			}
			if header.Version == 1 {
				return storedState{}, 0, fmt.Errorf(
					"legacy session format version 1 cannot be resumed",
				)
			}
			if header.Version != formatVersion {
				return storedState{}, 0, fmt.Errorf("unsupported session format version %d", header.Version)
			}
			if err := validateSessionID(header.Session.ID); err != nil {
				return storedState{}, 0, err
			}
			if header.Session.CreatedAt.IsZero() {
				return storedState{}, 0, fmt.Errorf("session %q creation time is zero", header.Session.ID)
			}
			value = newStoredState(header.Session.ID, header.Session.CreatedAt)
		case recordItem:
			if lineIndex == 0 {
				return storedState{}, 0, fmt.Errorf("session record must be first")
			}
			var itemValue itemRecord
			if err := json.Unmarshal(record.Data, &itemValue); err != nil {
				return storedState{}, 0, fmt.Errorf("decode item record: %w", err)
			}
			wantSequence := sessionstore.Sequence(itemCount + 1)
			if itemValue.Item.Sequence != wantSequence {
				return storedState{}, 0, fmt.Errorf(
					"item %d has sequence %d, want %d",
					itemCount,
					itemValue.Item.Sequence,
					wantSequence,
				)
			}
			if itemValue.Item.RecordedAt.IsZero() {
				return storedState{}, 0, fmt.Errorf("item %d recorded time is zero", itemCount)
			}
			if itemValue.Item.Kind != sessionstore.ItemToolCallStatus && len(itemValue.Operations) != 0 {
				return storedState{}, 0, fmt.Errorf(
					"item %d of kind %q initializes operations",
					itemCount,
					itemValue.Item.Kind,
				)
			}
			itemCount++
			if itemValue.Item.Kind == sessionstore.ItemFork {
				inheritedItemCount = itemCount
			}
			records = append(records, decodedRecord{Type: recordItem, Item: itemValue})
		case recordOperation:
			if lineIndex == 0 {
				return storedState{}, 0, fmt.Errorf("session record must be first")
			}
			var update operationRecord
			if err := json.Unmarshal(record.Data, &update); err != nil {
				return storedState{}, 0, fmt.Errorf("decode operation record: %w", err)
			}
			records = append(records, decodedRecord{
				Type: recordOperation, Operation: update.Operation,
			})
		default:
			return storedState{}, 0, fmt.Errorf("unsupported record type %q", record.Type)
		}
	}

	replayedItems := 0
	for recordIndex, record := range records {
		switch record.Type {
		case recordItem:
			itemIndex := replayedItems
			replayedItems++
			if itemIndex < inheritedItemCount {
				if len(record.Item.Operations) != 0 {
					return storedState{}, 0, fmt.Errorf("inherited item %d initializes operations", itemIndex)
				}
				value.inheritItem(record.Item.Item)
				continue
			}
			if err := replayItem(&value, record.Item); err != nil {
				return storedState{}, 0, fmt.Errorf("apply item %d: %w", itemIndex, err)
			}
		case recordOperation:
			if replayedItems < inheritedItemCount {
				return storedState{}, 0, fmt.Errorf(
					"operation record %d appears in inherited history",
					recordIndex+2,
				)
			}
			if err := value.saveOperation(record.Operation); err != nil {
				return storedState{}, 0, fmt.Errorf("apply operation record: %w", err)
			}
		}
	}
	return value, committedSize, nil
}
