# uah-core

uah-core is the agent runtime under [uah](https://github.com/viktordanov/uah).
It runs a model and its tools as one event loop in which every tool call is
asynchronous and durable:

- **Coordinator.** Persists accepted inputs, runs LLM turns, translates tool
  calls into operations, and wakes the model when results arrive. While calls
  run, the model keeps working.
- **Durable operations.** Tool calls become serializable, versioned operations
  that an operation manager runs and that survive a crash or a resume.
- **Session store.** Append-only, versioned session files that can be resumed
  and forked.
- **Responses client.** One client for the OpenAI Responses API and compatible
  providers (OpenAI, the ChatGPT Codex backend, OpenRouter, Fireworks, Ollama).
  It encodes request history once and reuses it across requests, and it
  supports custom tools with free-form input.
- **Responses WebSocket.** `responsesapi.WebSocketTransport` is an
  `http.RoundTripper` for the client that sends each request over one
  WebSocket, as Codex does with the ChatGPT backend, and answers with the
  server's events as an SSE stream. A request that extends the last one,
  with the same fields and the last input and output first, sends only its
  new input with `previous_response_id`. Any other request, the first on a
  new connection, and one after a response that did not complete send the
  whole input. A request while another is in flight goes over HTTP, and so
  does every request after a 426 to the upgrade or three WebSocket failures
  in a row.
- **Developer messages.** An `inbox.InputDeveloper` input is the harness's
  own message, such as context it prepares for a session. It is recorded,
  resumed, and forked like a user message and goes to the model as a
  `developer` role message, but it does not request a model response: it goes
  with the next request.
- **Effort updates.** `coordinator.Dependencies.EffortUpdate` can set a
  turn's reasoning effort with a Codex-style `configuration_update` input
  item (`llm.ItemConfigurationUpdate`) instead of the request's effort, so
  the request keeps its effort and the provider's prompt cache. The turn
  records the effort, so the update is resumed and forked with the history.
  `llm.Request.WithoutConfigurationUpdates` gives the request for a model
  that does not take the item, and `llm.Request.Effort` the effort the
  model reasons at.
- **Continuing when idle.** `coordinator.Dependencies.Continue` is asked
  for more input when a run with a stop-when-idle control goes idle, before
  it stops. The inputs it returns are recorded and handled as inbox inputs,
  and the run goes on; none lets it stop. A hard stop never asks.
- **Wake policy.** `coordinator.WakePolicy` can hold a turn's results so the
  model wakes once with all of them, and opens a valve with the output so far
  of a call that runs past the hold.

The repository has:

- [harness/](harness/): the library.
- [cmd/uah-core-runner](cmd/uah-core-runner/): a headless runner that executes
  one request and writes the session as JSONL.

## Origins

uah-core started from
[unreal-agent](https://github.com/unreallabsai/unreal-agent) v0.2.0 (MIT,
Copyright (c) 2026 Unreal Labs), whose license and notice it keeps, and has
gone its own way since: request encoding and resume performance, custom tools
with free-form input, developer-role inputs, text verbosity, effort changes
that keep the prompt cache, and the wake policy. It is developed on its own
and does not follow unreal-agent.

## Glossary

- **Input**: an event with a caller-supplied globally unique ID that remains
  stable across redeliveries.
- **Inbox**: session-scoped, in-memory deduplication of external (user),
  developer, control, and crash inputs.
- **Session**: append-only persisted history that can be forked.
- **LLM turn**: the coordinator-managed sequence around one logical LLM request.
- **Tool**: a capability described by a schema and bound to a translator.
- **Tool call**: a model-produced request to use a tool.
- **Tool translator**: validates a tool call and translates it into one or more
  operations. It runs synchronously on the coordinator's event loop and must not
  perform I/O or suspend the loop.
- **Tool call status**: the translation outcome: a validation error or references
  to submitted operations. Operation execution state is tracked separately;
  the translator formats these into a model-facing result.
- **Operation**: a serializable description of work produced by a tool translator
  for asynchronous execution. Implementations are encouraged to use the available
  [primitives](harness/primitives/).

## Components

| Component | Responsibility |
| --- | --- |
| Session inbox | Volatile, session-scoped input idempotency. |
| Coordinator | Persist accepted inputs, run LLM turns, resolve tool translators through the registry, and dispatch committed operations. |
| Session store | Persist canonical session history and operation state; support recovery and forks; atomically record tool-call status with operations. |
| Context builder | Statefully assemble model input in memory. Return the model input together with a record of anything omitted, truncated, or compacted. Perform no I/O and accept no persistence dependencies. |
| LLM Adapter | Send prepared model input to a provider and return a normalized completed response. Own authentication, cancellation, and provider errors. |
| Tool registry | Own the fixed Bash, ViewImage, and skill-use definitions and their translators; expose the host-selected set. |
| Tool translator | Validate a tool call and produce its status and operations. Format a recorded call status and prepared operation output into model results. Perform no I/O. |
| Operation manager | Actor runtime for durable operations. The local implementation is swappable. |

## Extending the harness

Runtime components are composable, and alternative implementations of their interfaces are encouraged.

We intend to preserve these invariants:

- Session-store items are serializable, and the storage format is versioned.
- We'll do our best to maintain backwards compatibility for sessions.
  An unsupported session version will always cause an explicit error on resume.
- Operations are versioned and always serializable.

For example, a proxy operations manager can send serialized operations to a
local operations manager running in a process inside a remote sandbox, allowing
tools to execute there.
