package coordinator

import (
	"time"

	"github.com/viktordanov/uah-core/harness/llm"
)

func (p WakePolicy) callHold(call llm.ToolCall) time.Duration {
	if p.QuietHold != nil {
		if hold := p.QuietHold(call); hold > 0 {
			return hold
		}
	}
	return p.Hold
}
