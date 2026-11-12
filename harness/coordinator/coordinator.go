// Package coordinator defines the owner of the central event loop.
package coordinator

import (
	"context"
	"time"

	"github.com/viktordanov/uah-core/harness/contextbuilder"
	"github.com/viktordanov/uah-core/harness/inbox"
	"github.com/viktordanov/uah-core/harness/llm"
	"github.com/viktordanov/uah-core/harness/operation"
	"github.com/viktordanov/uah-core/harness/session"
	"github.com/viktordanov/uah-core/harness/sessionstore"
	"github.com/viktordanov/uah-core/harness/tool"
)

type Dependencies struct {
	ToolHeartbeatInterval time.Duration
	SessionID             session.ID
	Inbox                 *inbox.Inbox
	Restored              sessionstore.ResumeState
	Sessions              sessionstore.Store
	ContextBuilder        contextbuilder.Builder
	LLM                   llm.Adapter
	Tools                 tool.Registry
	Operations            operation.Manager
	// Wake tunes when finished tool calls wake the model. Its zero value
	// keeps the default: after the grace period, each result wakes it.
	Wake WakePolicy
	// EffortUpdate, when set, gives the effort to set with a configuration
	// update before a turn's request, from the request built for it; ""
	// sets none. The turn records the effort, so the update stays in the
	// history and the request keeps its own effort, and with it the
	// provider's prompt cache.
	EffortUpdate func(llm.Request) llm.ReasoningEffort
	// Continue, when set, is asked for more input when the session goes
	// idle with a stop-when-idle control, before Run stops: the inputs it
	// returns are handled as inbox inputs and the loop goes on; none lets
	// Run stop. It runs on the loop's goroutine and should return once ctx
	// is done.
	Continue func(context.Context) []inbox.Input
}

// WakePolicy holds a turn's results back so that the model is not woken
// just to hear that a call is still running. Its zero value keeps the
// default grace period. An inbox input, such as a user message or a
// heartbeat, always wakes the model.
type WakePolicy struct {
	// Hold, when set, holds a turn's results, its immediate ones included,
	// until every call the turn issued has finished. A call still running
	// after Hold, from any turn, wakes the model with its output so far and
	// stops holding the turn back.
	Hold time.Duration
	// QuietHold optionally overrides Hold for an explicitly blocking call.
	// A positive duration also suppresses automatic tool heartbeats for
	// that call until its deadline; user input and other calls still wake
	// the model normally. Zero or negative uses Hold. The callback must be
	// pure: it may be called more than once for the same call.
	QuietHold func(llm.ToolCall) time.Duration
	// Progress renders the output so far of a call that outlived Hold; nil
	// or an empty result shows no output.
	Progress func([]operation.Operation) string
}

type Coordinator interface {
	// Run owns one session's decision loop until a stop control completes or
	// the context is canceled. It returns nil for a completed stop. It is
	// single-use; its caller must cancel the Inbox when Run returns.
	Run(context.Context) error
}

func New(dependencies Dependencies) Coordinator {
	return &coordinator{
		dependencies: dependencies,
		state:        newLoopState(),
	}
}
