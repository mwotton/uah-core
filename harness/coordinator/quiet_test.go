package coordinator

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/viktordanov/uah-core/harness/inbox"
	"github.com/viktordanov/uah-core/harness/llm"
	"github.com/viktordanov/uah-core/harness/operation"
)

func quietPolicy() WakePolicy {
	p := holdPolicy(5 * time.Minute)
	p.QuietHold = func(call llm.ToolCall) time.Duration {
		if call.CallID == "quiet" {
			return time.Hour
		}
		return 0
	}
	return p
}

func TestQuietHoldSuppressesValveAndHeartbeatsUntilCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.current.dependencies.ToolHeartbeatInterval = 10 * time.Minute
		run.respond(t, 0, toolGraceResponse("quiet"))
		synctest.Sleep(59 * time.Minute)
		if run.requestCount() != 1 {
			t.Fatal("the deliberate wait woke the model before completion")
		}
		updateToolGraceCall(t, run, "quiet", operation.StatusCompleted)
		if run.requestCount() != 2 {
			t.Fatal("completion did not wake the model")
		}
		assertStopResult(t, run.calls[1].request, "quiet", string(operation.StatusCompleted))
	})
}

func TestQuietHoldDeadlineStillWakes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.respond(t, 0, toolGraceResponse("quiet"))
		synctest.Sleep(time.Hour + 10*slurpIdleTimeout)
		if run.requestCount() != 2 {
			t.Fatal("the quiet hold's deadline did not wake the model")
		}
		assertStopResult(t, run.calls[1].request, "quiet", "Still running after 60 minutes. The call continues in the background, and its result arrives in a later turn.\nOutput so far:\npartial")
	})
}

func TestQuietHoldAllowsUserInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.respond(t, 0, toolGraceResponse("quiet"))
		synctest.Sleep(time.Minute)
		run.input(t, externalEvent(t, 1, "steer", "check the job"))
		if run.requestCount() != 2 {
			t.Fatal("the quiet hold blocked user input")
		}
	})
}

func TestQuietHoldDoesNotSuppressOtherCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.current.dependencies.ToolHeartbeatInterval = time.Minute
		run.respond(t, 0, toolGraceResponse("quiet", "ordinary"))
		synctest.Sleep(time.Minute + 10*slurpIdleTimeout)
		if run.requestCount() != 2 || countHeartbeatMessages(run.calls[1].request) != 1 {
			t.Fatal("the quiet call suppressed the ordinary call's heartbeat")
		}
		calls := run.current.heartbeatCalls()
		if len(calls) != 1 || calls[0].CallID != "ordinary" {
			t.Fatalf("heartbeat calls = %v, want only ordinary", calls)
		}
	})
}

func TestQuietHoldDoesNotChangeOrdinaryValve(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.respond(t, 0, toolGraceResponse("quiet", "ordinary"))
		synctest.Sleep(5*time.Minute + 10*slurpIdleTimeout)
		if run.requestCount() != 2 {
			t.Fatal("the quiet call delayed the ordinary call's valve")
		}
		assertStopResult(t, run.calls[1].request, "ordinary", stillRunningFiveMinutes)
	})
}

func TestQuietHoldAllowsHardStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		run := newWakeTestRun(t, quietPolicy())
		run.respond(t, 0, toolGraceResponse("quiet"))
		run.input(t, stopInput(t, "stop", inbox.StopHard))
		if len(run.operations.cancels) != 1 {
			t.Fatal("the quiet hold blocked cancellation")
		}
		updateToolGraceCall(t, run, "quiet", operation.StatusCanceled)
		run.assertStopped(t)
		if run.requestCount() != 1 {
			t.Fatal("cancellation woke the model")
		}
	})
}
