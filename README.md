# LLM Shared-Bus Turn Allocation Protocol (LTAP)
### Technical Specification — Request for Proposal — Version 1.0 Draft

> **Normative source.** This README is the normative protocol text. `index.html` is a styled rendering of the same content and `ltap-animation.html` an interactive companion; where they disagree, this file governs.

## Implementations

| Project | Role |
|---|---|
| [LTAP-SDK](https://github.com/minimumbuilds/LTAP-SDK) | Reference implementation: Arbiter, participant base class, in-process transport, observability emitters, and a test suite covering the protocol invariants and multi-tick arbitration scenarios. |
| [LTAP-RP](https://github.com/minimumbuilds/LTAP-RP) | LTAP-over-Kafka transport binding and containerised multi-agent demo (Redpanda broker, Ollama-backed agents, live chat UI). |
| Multi-agent room simulation | Full application deployment — LLM personas conversing and moving between rooms, one LTAP channel per room. Source of the §11 field notes. |

---

## 1. Introduction

Language models cannot meaningfully abstain: every inference call produces tokens, and discarding output is not equivalent to silence — it creates hidden state divergence between agents that share a context. The conventional mitigation, routing turns through a coordinating LLM, trades one problem for another: the coordinator is itself a generative agent, one with inference cost and the potential for its own turn-allocation agenda.

LTAP removes the coordinator. Turn allocation is programmatic, driven entirely by bids the channel participants themselves declare. A non-generative Arbiter resolves contention and maintains a shared append-only log that all participants read from equally. Who speaks next is a function of participant priority signals — not another model's inference.

This document specifies a **tick-based contention protocol** for arbitrating transmission rights among a set of LLM participants sharing a common communication bus. Each tick executes in two logical phases: a **bid phase** (Phases 1–4), in which all participants declare intent and the Arbiter selects a winner without producing any output, and an **execution phase** (Phases 5–6), in which the winner transmits and the result is broadcast. The protocol prevents concurrent transmission and response flooding without imposing any constraint on participant role, domain, or output content.

The protocol is intentionally analogous in scope to a media-access control layer. Participant identity, purpose, output semantics, and internal context management are out of scope. The protocol defines only: how participants declare transmission intent, how that contention is resolved, and how the result is broadcast to all bus members.

**Arbiter separation is a foundational requirement of this protocol.** A participant that evaluates bids has a direct incentive to manipulate contention in its own favour. Bid evaluation must therefore be performed by a neutral process that is architecturally separate from the participant set, holds no generative role, and submits no bids of its own. This process is called the Arbiter. All phases of the tick loop are owned and executed exclusively by the Arbiter.

**Cooperative assumption.** LTAP assumes participants report priority honestly. The protocol provides no mechanism to detect or penalise a participant that always bids 1.0. Deployments involving adversarial or incentive-aware participants must implement application-layer mechanisms — reputation scoring, transmission budgets, quotas, or similar — outside the scope of this specification.

**Consistency model.** LTAP provides per-channel sequential consistency: within a channel, all state transitions are serialized through the Arbiter's tick cycle, and only protocol-compliant messages contribute to the channel log. Any out-of-schema or extraneous token generation — even if discarded locally — creates risk of hidden state divergence between participants and MUST be either prevented at generation time or provably contained at the client layer; the two participant conformance tiers in §4.2 define what each posture requires. This is the foundational justification for strict output control at bid-generation time and explains why free generation followed by post-hoc filtering satisfies neither tier.

---

## 2. Definitions

| Term | Definition |
|---|---|
| `Participant` | An LLM-backed agent registered on one or more channels. Participants bid and transmit; they do not evaluate. |
| `Arbiter` | The neutral process that owns the tick loop for every channel, issues bid requests, evaluates contention, selects the winner, signals transmission rights, and broadcasts events. The Arbiter is not a participant and does not produce transmissions of its own. |
| `Channel` | A discrete broadcast domain managed independently by the Arbiter. Each channel has its own participant set, log, and tick counter. A participant may be registered on multiple channels simultaneously; its arbitration state (cooldown, ineligibility, failure streak) is tracked separately per channel. |
| `Bus` | The aggregate passive state managed by the Arbiter: the collection of all active channels. |
| `Tick` | One indivisible arbitration cycle within a single channel, driven by the Arbiter. Tick semantics are logical, not wall-clock. Channels tick independently. |
| `Bid phase` | Phases 1–4 of a tick: ineligibility decrement, bid collection, bid weighting, and winner selection. No participant output is produced. |
| `Execution phase` | Phases 5–6 of a tick: winner transmission and event broadcast. |
| `BidRequest` | A message sent by the Arbiter to a participant to open the bid phase of a tick on a specific channel. |
| `Bid` | A structured intent declaration returned by a participant in response to a BidRequest. Bids are opaque to all other participants until the tick concludes. |
| `TransmissionResponse` | The message returned by the winning participant when signalled, carrying its output content and optional addressee. |
| `LogEntry` | A record appended to a channel's log; either a `ParticipantTransmission` or a `SystemEvent`. |
| `Bus Event` | A structured message broadcast by the Arbiter to participants after each tick, carrying a transmission or an infrastructure notification, scoped to the channel on which the event occurred. |
| `Cooldown` | A per-participant, per-channel dampening window covering the `COOLDOWN_TICKS` ticks that follow a participant's transmission on that channel. Not a stored counter: the Arbiter derives it from `last_acted_tick` as `channel.tick − last_acted_tick ≤ COOLDOWN_TICKS` (§4.3). A participant that has never transmitted on the channel has no cooldown window. |
| `Direct address` | A transmission explicitly directed at a named participant. Triggers a strong priority bias for that participant on every subsequent tick of the same channel until another `ParticipantTransmission` on that channel supersedes it as the most recent log entry. |
| `Equivalent Mechanism` | A generation-time constraint mechanism that satisfies the constrained decoding requirement (§4.2). An equivalent mechanism guarantees: (1) zero emission of out-of-schema tokens; (2) bounded output strictly conforming to the Bid Schema (§3.3.1); and (3) no transient generation of discardable content — the model must not internally generate and then discard out-of-schema tokens, as this risks hidden state divergence even when only conforming output is emitted. Tier S participants (§4.2) satisfy all three properties; Tier H participants satisfy (1)–(2) at generation time and contain (3) at the client layer. Post-generation filtering or validation alone does not qualify for either tier. |

---

## 3. Data Model

### 3.1 Participant

```
Participant {
  id:               ParticipantId
  ineligible_ticks: int           // ticks before participant may bid again; Arbiter-maintained
  failure_streak:   int           // consecutive transmission failures; maintained by the Arbiter
  last_acted_tick:  int | null    // tick of most recent transmission on this channel; null until first transmission
}
```

There is no stored cooldown counter: the cooldown window (§4.3) is derived from `last_acted_tick` at weighting time. `last_acted_tick` initialises to `null` on registration, so a participant that has never transmitted is never dampened.

The Participant record contains only the state required for arbitration. How a participant stores its own history, builds its internal context, or manages memory are client-side concerns outside this protocol.

### 3.2 BidRequest

```
BidRequest {
  tick:            int
  channel_id:      ChannelId
  participant_id:  ParticipantId
  intent_version:  string    // deployment-defined identifier for the IntentTag set in use
}
```

The BidRequest is the complete protocol-level message the Arbiter sends to open the bid phase for a participant on a specific channel. `channel_id` identifies which channel is soliciting the bid; a participant registered on multiple channels will receive separate BidRequests per channel per tick. `intent_version` allows participants to confirm they are using the same `IntentTag` enumeration as the Arbiter; its format and semantics are deployment-defined. Any additional application context provided alongside the BidRequest is outside this specification. Transport is implementation-defined.

### 3.3 Bid

```
Bid {
  want_to_send: bool
  priority:     float [0.0, 1.0]    // raw value, before Arbiter weighting
  intent:       IntentTag           // advisory; does not affect arbitration weight
}
```

The `participant` field is absent from the Bid response payload. The Arbiter derives participant identity from the BidRequest context; it is not the participant's responsibility to self-identify in the payload.

`IntentTag` is a closed enumeration specified by the deployment. The base protocol defines one reserved value: `"pass"` (explicit abstention, equivalent to `want_to_send: false`). All other values are advisory metadata with no effect on contention arithmetic.

> **Bid fields are a control plane, not a data plane.** Bid fields — including `intent` — MUST NOT be used to encode arbitrary payloads, hidden data, or context between participants. Implementations MUST ensure bids remain non-expressive and strictly bounded to their protocol-defined semantics (see §7, Invariant 14).

`addressed_to` is absent from the Bid. Who a participant intends to address is a transmission-time decision with no bearing on contention evaluation; it is declared in the TransmissionResponse (§3.4).

#### 3.3.1 Bid Schema

The following JSON Schema (draft-07) is the normative structural definition of a Bid payload, referred to throughout this specification as the **Bid Schema**:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["want_to_send", "priority", "intent"],
  "additionalProperties": false,
  "properties": {
    "want_to_send": { "type": "boolean" },
    "priority":     { "type": "number", "minimum": 0.0, "maximum": 1.0 },
    "intent":       { "type": "string" }
  }
}
```

`intent` is constrained to `string` at the base level. Deployments MAY extend the Bid Schema to restrict `intent` to the valid `IntentTag` enum values for their `intent_version`. The Arbiter SHOULD publish the deployment-specific extended schema alongside or via the `intent_version` identifier so participants can apply the tighter constraint. An unrecognised `intent` value remains subject to the Arbiter-side substitution rule in §4.2 regardless of whether the extended schema is in use.

### 3.4 TransmissionResponse

```
TransmissionResponse {
  content:      string
  addressed_to: ParticipantId | null
}
```

Returned by the winning participant when signalled by the Arbiter in Phase 5. Validation rules are specified in §4.5.

### 3.5 Log Entries

`bus.log` is a sequence of `LogEntry` values. A `LogEntry` is one of two distinct types:

```
ParticipantTransmission {
  type:         "participant"
  tick:         int
  sender:       ParticipantId
  content:      string
  addressed_to: ParticipantId | null
}

SystemEvent {
  type:         "system"
  tick:         int
  content:      string
}
```

Separating participant transmissions from system events at the type level eliminates the `sender: "system"` sentinel and makes log queries unambiguous. The direct-address rule (§4.3) reads only `ParticipantTransmission` entries; system events are never consulted for arbitration.

### 3.6 Bus Event

```
BusEvent {
  type:         "transmission" | "system"
  tick:         int
  channel_id:   ChannelId
  sender:       ParticipantId | null    // null on system events
  content:      string
  addressed_to: ParticipantId | null    // set on transmission events only
  is_self:      bool                    // true when sender == receiving participant
  bid_request:  BidRequest | null       // non-null when recipient is eligible to bid next tick
}
```

`is_self` lets a participant distinguish its own winning transmission from those of others without the Arbiter needing visibility into the participant's internal context.

`bid_request` carries the solicitation for the upcoming tick directly inside the BusEvent, eliminating a separate round trip. It is non-null when the receiving participant will be eligible to bid in the next tick (i.e., `ineligible_ticks` after the next Phase 1 decrement will be 0). It is null for ineligible participants and for system events where the Arbiter cannot yet determine next-tick eligibility (such as membership events at the tick boundary). When `bid_request` is non-null, the participant's response to the BusEvent serves as its bid; no separate BidRequest is issued for that participant that tick.

### 3.7 Channel and Bus

```
Channel {
  id:           ChannelId
  participants: dict[ParticipantId → Participant]    // per-channel arbitration state
  log:          list[LogEntry]                       // append-only; written only by the Arbiter
  tick:         int                                  // initialized to 0; per-channel
}

Bus {
  channels:     dict[ChannelId → Channel]
}
```

`Channel` is the unit of arbitration. Each channel maintains an independent participant set, log, and tick counter. A `ParticipantId` may appear in multiple channels; the `Participant` record stored per channel holds only that channel's arbitration state — cooldown, ineligibility, and failure streak are not shared across channels.

`Bus` is the aggregate of all active channels. It is passive state; all reads and writes are performed by the Arbiter.

### 3.8 Arbiter

```
Arbiter {
  bus:                        Bus
  COOLDOWN_TICKS:             int     // dampened-bid window after transmitting; reference: 2
  DAMPENING_FACTOR:           float   // cooldown multiplier, §4.3 step 1; range [0.0, 1.0]; reference: 0.3
  ADDRESS_BIAS:               float   // direct-address multiplier, §4.3 step 2; range ≥ 0.0; reference: 2.0
  ELIGIBILITY_THRESHOLD:      float   // weighted priority must exceed this to contend, §4.4; range [0.0, 1.0); reference: 0.1
  TIE_BAND:                   float   // near-tie band width for winner selection, §4.4; range [0.0, 1.0]; reference: 0.05
  BID_TIMEOUT:                float   // seconds to wait for a bid response; reference: 5.0
  TRANSMISSION_TIMEOUT:       float   // seconds to wait for a TransmissionResponse; reference: 60.0
  MAX_CONSECUTIVE_FAILURES:   int     // failures before ineligibility; reference: 2
}
```

All weighting and selection constants are deployment-configurable parameters with the reference values above as defaults; the arithmetic in §4.3 and §4.4 is expressed in terms of these names.

> **Parameter floor values.** `COOLDOWN_TICKS = 0` disables cooldown dampening: the window `0 < channel.tick − last_acted_tick ≤ 0` is empty, so no bids are ever dampened. `DAMPENING_FACTOR = 1.0` also disables dampening (the multiplier is identity); `DAMPENING_FACTOR = 0.0` turns the cooldown window into a hard mute — the weighted priority becomes 0 and fails the eligibility threshold, reproducing a lockout model as configuration. `ADDRESS_BIAS = 1.0` disables the direct-address bias. `TIE_BAND = 0.0` restricts the contender set to exact ties. `MAX_CONSECUTIVE_FAILURES = 0` means every transmission failure immediately imposes ineligibility, since `failure_streak >= 0` is always true. All are valid configurations for their respective disabling or maximum-strictness effects. Negative values are not permitted.

> **Cooldown window semantics.** `COOLDOWN_TICKS = N` means the participant's bids are dampened on exactly the N ticks following a transmission. The window is derived at weighting time — `last_acted_tick ≠ null` and `channel.tick − last_acted_tick ≤ COOLDOWN_TICKS` — rather than maintained as a stored counter, so there is nothing to decrement and no offset to compensate for. (A `+1` offset applies only to `ineligible_ticks` (§4.5), because Phase 1 decrements it before the Phase 2 eligibility check.)

The Arbiter's mutable state is limited to the Bus and per-participant ineligibility and failure-streak counters plus `last_acted_tick` (all per-channel). It must hold no participant-influenced *arbitration state* beyond the protocol records defined here, and must not semantically interpret participant content beyond `addressed_to`.

### 3.9 MemberListResponse

```
MemberListResponse {
  channel_id:   ChannelId
  tick:         int                   // channel.tick at the moment the snapshot was taken
  participants: list[ParticipantId]   // all participants currently registered on the channel
}
```

The Arbiter sends a `MemberListResponse` in two contexts: as a mandatory part of the registration acknowledgment (§5.1), and in response to an explicit participant query (§5.4). In both cases the list is a point-in-time snapshot of `channel.participants` at the moment the Arbiter processes the request; participants who join or leave after the snapshot is taken are not reflected until the next join/leave `BusEvent` or the next query.

---

## 4. Protocol Phases

Each tick is driven entirely by the Arbiter and consists of a **bid phase** (Phases 1–4) followed by an **execution phase** (Phases 5–6). Participants respond to BidRequests; they do not initiate phases.

The Arbiter runs an independent tick loop for every channel. Channel tick loops are concurrent with respect to each other; their relative ordering and scheduling are implementation-defined. The phases described below apply to a single channel's tick loop.

```
// Each channel runs this loop independently:
foreach channel in bus.channels:

  foreach tick:                              // driven by the Arbiter

    // — Bid phase —
    Phase 1:  Decrement counters            (ineligible_ticks for all participants)
    Phase 2:  Collect bids                  (solicit eligible participants in parallel; wait up to BID_TIMEOUT)
    Phase 3:  Weight bids                   (deterministic; no external calls)
    Phase 4:  Select winner                 (deterministic + stochastic tie-break)

    // — Execution phase —
    Phase 5:  Signal and receive            (signal winner; wait up to TRANSMISSION_TIMEOUT)
    Phase 6:  Broadcast events              (append to channel.log; best-effort broadcast to participants)

    // — Tick boundary —
    Boundary: Apply queued membership changes   (SystemEvents and broadcasts; tick = channel.tick)
    channel.tick += 1
```

Membership changes queued during a tick are applied at the Boundary step, after Phase 6 completes and before `channel.tick` is incremented. All `SystemEvent` entries and membership `BusEvent` broadcasts produced at that boundary carry `tick = channel.tick` — the number of the tick that just completed, not the tick about to begin.

### 4.1 Phase 1 — Decrement Counters

The Arbiter decrements every participant's `ineligible_ticks` by one, floored at zero, for the channel being ticked. (Cooldown involves no counter and nothing to decrement; it is derived from `last_acted_tick` in Phase 3.) This phase runs unconditionally regardless of channel occupancy. When `channel.participants` is empty the tick still completes normally — no bids are collected, no winner is selected, and `channel.tick` advances at the Boundary step. This preserves consistent tick numbering regardless of occupancy.

### 4.2 Phase 2 — Bid Collection

The Arbiter collects bids from every **eligible** participant in parallel and waits up to `BID_TIMEOUT` seconds for each response. A participant is eligible for solicitation when `ineligible_ticks == 0`. Ineligible participants are not solicited; they receive an implicit safe default for the tick and may become eligible again once `ineligible_ticks` reaches zero.

**Bundled vs. standalone BidRequests.** When the preceding tick produced a transmission, the Arbiter MUST embed the BidRequest in the `bid_request` field of the BusEvent delivered in Phase 6 of that tick (§4.6), rather than sending a separate BidRequest message. The participant's bid response to the BusEvent is treated as its bid for the current tick; the `BID_TIMEOUT` window starts from when the BusEvent was sent. A standalone BidRequest is issued only when no BusEvent was sent in the preceding tick (i.e., the preceding tick produced no transmission), or for participants who joined at the preceding tick boundary and therefore did not receive that BusEvent.

How each participant generates its bid is outside the protocol's scope, except as specified below.

**Constrained generation requirement.** A conforming participant MUST produce its bid under a generation-time structural constraint against the Bid Schema (§3.3.1) — never by free generation followed by post-hoc validation or filtering. When the Arbiter has published a deployment-specific extended schema (§3.3.1), participants SHOULD use that schema in place of the base Bid Schema.

This requirement exists to: (1) prevent covert or unintended data transmission via bid fields, including `intent`; (2) eliminate discarded-token divergence — a model that internally generates and then discards out-of-schema tokens may produce hidden state that diverges across participants, even when only conforming output is emitted; and (3) minimize unnecessary token generation and its associated cost and latency.

**Participant conformance tiers.** Not every deployment controls its inference stack deeply enough to guarantee all three properties of an Equivalent Mechanism (§2). Two tiers are defined; a deployment MUST declare which tier its participants meet.

- **Tier S (Strict).** The participant satisfies the full Equivalent Mechanism definition, including property (3): no transient generation of discardable content. This requires constraint enforcement inside the decoding loop (logit-level grammar/FSM masking) and is generally available only with self-hosted inference. Tier S is REQUIRED for participants that share mutable context or reuse model state across calls, where transient generation materialises as hidden state divergence.

- **Tier H (Hosted).** The participant uses a provider- or runtime-supplied structured-output mechanism that guarantees properties (1) and (2) — zero out-of-schema *emission*, bounded schema-conforming output — but cannot attest to property (3) (e.g. hosted APIs and local model servers whose schema enforcement constrains the emitted message, not internal sampling; reasoning models that produce non-emitted thinking tokens). Tier H participants MUST additionally guarantee, at the client layer: any non-conforming or discarded model content (reasoning traces, stripped preambles) is never appended to any context, log, or history that influences a future bid or transmission of any participant. Under Tier H the divergence risk of property (3) is contained rather than eliminated: each call's context is rebuilt deterministically from protocol-visible state, so transient generation cannot accumulate into divergent hidden state.

Post-generation validation alone — free generation plus a parse/filter step — satisfies neither tier.

**Effect on failure handling.** A structurally conforming participant cannot produce a bid that fails JSON parsing or violates the `want_to_send` boolean or `priority` numeric-range constraints. The corresponding substitution paths in the table below therefore function as Arbiter-side defense-in-depth against non-conforming participants and are not expected to fire for compliant ones. The timeout path remains unconditional.

The Arbiter must not include any other participant's bid, private state, or bus events that the solicited participant did not receive as part of any application context accompanying the BidRequest.

Each participant must respond with a JSON object conforming to:

```json
{
  "want_to_send": true | false,
  "priority":     0.0..1.0,
  "intent":       "<IntentTag>"
}
```

**Timeout and failure handling.** If a participant does not respond within `BID_TIMEOUT`, or the response cannot be parsed as JSON, the Arbiter substitutes the safe default for that participant without retrying within the same tick:

```json
{ "want_to_send": false, "priority": 0.0, "intent": "pass" }
```

**Semantic validation.** After successful JSON parsing, the Arbiter validates and normalises each field before weighting:

| Field | Invalid condition | Resolution | Note |
|---|---|---|---|
| `priority` | Non-finite (`NaN`, `Infinity`) | Substitute full safe default | Unreachable for conforming participants |
| `priority` | Outside `[0.0, 1.0]` but finite | Clamp to `[0.0, 1.0]` | Unreachable for conforming participants |
| `want_to_send` | Not a boolean | Substitute full safe default | Unreachable for conforming participants |
| `intent` | Not a recognised `IntentTag` | Substitute `intent: "pass"`; preserve `want_to_send` and `priority` | |

An unrecognised `intent` is treated as advisory-unknown: the Arbiter substitutes `"pass"` as the intent label but preserves `want_to_send` and `priority` unchanged. Because `intent` is advisory with no effect on contention arithmetic (§3.3), a version-skew mismatch on this field does not silence an otherwise valid bid.

A timed-out or defaulted participant remains registered and eligible, and may bid normally in subsequent ticks. Repeated timeouts may be used as grounds for deregistration (§5.2) under implementation-defined policy.

Bids are held privately by the Arbiter and are never disclosed to other participants.

### 4.3 Phase 3 — Bid Weighting

The Arbiter post-processes raw priority values through a deterministic pipeline. No external calls are made. Modifiers are applied in the order listed.

| Step | Operation | Condition |
|---|---|---|
| 1 | `priority × DAMPENING_FACTOR` | `participant.last_acted_tick ≠ null` and `channel.tick − participant.last_acted_tick ≤ COOLDOWN_TICKS` (cooldown window) |
| 2 | `priority × ADDRESS_BIAS` | Most recent `ParticipantTransmission` in `channel.log` has `addressed_to` == this participant |
| 3 | `priority = clamp(priority, 0.0, 1.0)` | Always; applied last |

Reference values: `DAMPENING_FACTOR = 0.3`, `ADDRESS_BIAS = 2.0` (§3.8). Worked examples below use the reference values.

**Rationale — step 1 (cooldown dampening).** Prevents monopoly without hard-locking turns. Multiplicative, not binary: a participant under cooldown may still win if its raw priority is high enough. The winner of a tick remains eligible on subsequent ticks; only its priority is dampened.

**Rationale — step 2 (direct-address bias).** Gives a priority bias to a participant explicitly addressed in the most recent participant transmission on this channel. System events never displace it. Self-address is suppressed at TransmissionResponse validation (§4.5). The bias is multiplicative so the addressee's own priority signal is preserved: a reluctant addressee (low raw priority) receives a nudge, not a command, and an addressee that bids `0.0` or `want_to_send: false` is not dragged into the conversation. This is a bias, not a guarantee.

> **Design note — why not a floor.** Earlier drafts of this specification used `priority = max(priority, 0.95)` here, applied after cooldown dampening. Field deployment showed this erases the addressee's own priority signal (any willing addressee jumps to 0.95 regardless of its raw bid) and, because the floor overrode dampening, produced persistent A→B→C→A address loops that cooldown could not break. The multiplicative form composes with step 1 — an addressed participant inside its cooldown window nets `× (DAMPENING_FACTOR × ADDRESS_BIAS)`, `× 0.6` at reference values — so directed threading is favoured without becoming self-sustaining.

**Ping-pong tradeoff.** When two participants consistently address each other, each receives the `ADDRESS_BIAS` multiplier in alternation, which favours turn alternation between them over any third participant whose raw bids are similar. Because the bias composes with cooldown dampening and preserves raw-priority ordering, a third participant with a sufficiently strong bid can still break in. Deployments needing stronger fairness for bystanders may lower `ADDRESS_BIAS`, or additionally cap consecutive directly-addressed wins at the application layer.

A bid is eligible only when `want_to_send == true` and weighted `priority > ELIGIBILITY_THRESHOLD` (reference: 0.1).

### 4.4 Phase 4 — Winner Selection

The Arbiter selects the winner from the weighted bid set:

```
eligible   = [b for b in bids if b.want_to_send and b.priority > ELIGIBILITY_THRESHOLD]
if eligible is empty → no transmission this tick; advance to Phase 6

top        = max(b.priority for b in eligible)
contenders = [b for b in eligible if b.priority >= top - TIE_BAND]
winner     = random.choice(contenders)
```

**At most one winner per tick.** If no eligible bids exist, the tick produces no transmission. Otherwise exactly one winner is selected and signalled.

**Randomness.** The selection algorithm is implementation-defined. Conformance level **Replayable** (§6) requires logging the full contender set and sufficient RNG state to reproduce the selection. No specific algorithm or seed strategy is mandated.

**Fairness.** LTAP provides starvation *resistance* through cooldown dampening and stochastic tie-breaking, but does not guarantee equal or proportional transmission share. The protocol is priority-based: a participant that consistently bids low may rarely win. This is intentional. The observability record (§6) provides the signal needed to detect starvation in practice.

### 4.5 Phase 5 — Signal and Receive

The Arbiter signals the winning participant that it holds transmission rights and waits up to `TRANSMISSION_TIMEOUT` seconds for a `TransmissionResponse`. How the participant produces its output is outside the protocol's scope.

**TransmissionResponse validation.** Upon receipt the Arbiter applies the following rules in order:

| Condition | Resolution |
|---|---|
| No response within `TRANSMISSION_TIMEOUT` | Transmission failure |
| Response not parseable | Transmission failure |
| `content` missing, null, or not a string | Transmission failure |
| `content` is an empty string | Transmission failure |
| `addressed_to` == sender (self-address) | Set to null; proceed |
| `addressed_to` not a registered participant | Set to null; proceed |
| `addressed_to` absent or null | Accepted as null; proceed |
| Extra fields present | Ignored |
| `content` exceeds implementation-defined size limit | Transmission failure |

**On transmission failure:** the Arbiter increments `winner.failure_streak`. If `failure_streak >= MAX_CONSECUTIVE_FAILURES`, the Arbiter sets `winner.ineligible_ticks = COOLDOWN_TICKS + 1` and resets `winner.failure_streak = 0`. This prevents a repeatedly failing participant from consuming `TRANSMISSION_TIMEOUT` on every tick when it lacks viable competitors. The tick advances without producing a bus event regardless.

**On successful transmission:** the Arbiter resets `winner.failure_streak = 0` before proceeding to Phase 6.

### 4.6 Phase 6 — Broadcast Events

**Log append.** On successful receipt of a valid `TransmissionResponse`, the Arbiter appends a `ParticipantTransmission` entry to `channel.log`. This append is the authoritative record of the tick's output and occurs before any broadcast attempt.

**Broadcast.** The Arbiter then attempts to deliver a `BusEvent` of type `"transmission"` (with `channel_id` set) to every participant in the channel (`is_self: true` for the sender, `is_self: false` for all others). Before sending, the Arbiter applies the post-transmission state updates for the winner (below), then pre-computes next-tick eligibility for each participant: eligible when `max(0, ineligible_ticks - 1) == 0`. For each eligible participant the Arbiter MUST set `bid_request` to the `BidRequest` for the next tick; for ineligible participants `bid_request` is null. The winner's `last_acted_tick` (not `ineligible_ticks`) is updated, so the winner remains eligible and receives a `bid_request` (its bids will be dampened by Phase 3 while inside the cooldown window). Broadcast is best-effort: delivery failures to individual participants are independent, do not affect delivery to others, and do not undo the log append. A participant that fails to receive a BusEvent remains registered on the channel; repeated delivery failures may be grounds for deregistration under implementation-defined policy. A participant that fails to receive its bundled `bid_request` is treated as a bid timeout for that tick (safe default applied).

The Arbiter sets, within the channel's participant record for the winner:
- `winner.last_acted_tick = channel.tick`
- `winner.failure_streak = 0`

No cooldown counter is written: setting `last_acted_tick` is what opens the winner's cooldown window (§4.3).

If Phase 5 produced no valid transmission, the Arbiter still advances `channel.tick` but appends no entry and broadcasts no event.

**Infrastructure events.** The Arbiter broadcasts a `BusEvent` of type `"system"` (with `channel_id` set) to all participants on the channel on every join or leave. This broadcast is unconditional and subject to the same best-effort delivery rules as transmission events. System events never trigger winner selection.

---

## 5. Membership

Membership is scoped to a channel. A participant registers on, and deregisters from, a specific channel. A participant may be registered on multiple channels simultaneously; its arbitration state on each channel is entirely independent. Membership changes take effect at a channel's tick boundary — never mid-tick. External requests received while that channel's tick is in progress are queued and applied in receipt order when the tick completes.

### 5.1 Registration

A participant registers on a specific channel. The Arbiter:

1. **Duplicate check.** If the `ParticipantId` is already present in `channel.participants` for the target channel, the Arbiter must reject the registration. The existing participant's record and counters are unchanged. The rejection response format is implementation-defined. The same `ParticipantId` may be registered on other channels without conflict.
2. Adds the participant to `channel.participants` with `ineligible_ticks = 0`, `failure_streak = 0`, `last_acted_tick = null`.
3. Appends a `SystemEvent` to `channel.log` with `tick = channel.tick` (the completing tick's number, before the increment).
4. Sends a registration acknowledgment to the registering participant. The acknowledgment must be sent before the channel's next tick begins. Its format is implementation-defined; it must include a `MemberListResponse` (§3.9) — providing the current participant list, `channel_id`, and `channel.tick` — so the participant arrives with a complete view of who is on the channel and which tick it will first be solicited on. The registering participant does not receive a join `BusEvent`; the acknowledgment is its sole confirmation.
5. Broadcasts a `BusEvent` of type `"system"` (with `channel_id` set) to all **existing** participants on that channel. This broadcast is unconditional. The registering participant does not receive its own join event.

When multiple registrations are queued at the same channel boundary, they are processed in receipt order. Each registration's `SystemEvent` is appended and broadcast before the next is processed.

The registering participant begins bidding on the channel in the next tick. It receives no retroactive history from `channel.log`; its starting state is whatever the client initialises it with.

### 5.2 Deregistration

A participant deregisters from a specific channel. A participant may leave voluntarily, or the Arbiter may remove it from a channel (e.g. on repeated timeouts or an external signal). All external deregistration requests are queued and applied at the next boundary of the relevant channel in receipt order.

The Arbiter:

1. Removes the participant from `channel.participants` for the target channel.
2. Appends a `SystemEvent` to `channel.log` with `tick = channel.tick` (the completing tick's number, before the increment).
3. Broadcasts a `BusEvent` of type `"system"` (with `channel_id` set) to all **remaining** participants on that channel. This broadcast is unconditional. The departing participant does not receive its own departure event.
4. Discards the departed participant's cooldown, ineligibility, and failure-streak state for that channel only. The participant's registration and state on any other channels are unaffected.

When multiple deregistrations are queued at the same channel boundary, they are processed in receipt order before any registrations queued at the same boundary.

If a participant with a queued removal was selected as winner during that channel's tick: if the participant is still reachable, Phase 5 may complete normally before the deregistration applies at the tick boundary; if the participant has become unreachable, Phase 5 fails and the standard transmission failure path applies (failure streak incremented, tick advances), after which the deregistration is applied at the boundary.

### 5.3 Channel Lifecycle

Channels are created and destroyed by the Arbiter. Channel creation and destruction are implementation-defined operations; their triggers, naming scheme, and access control are outside this specification.

When a channel is destroyed, the Arbiter:

1. Deregisters all participants currently on the channel, applying the deregistration procedure (§5.2) for each, in implementation-defined order.
2. Removes the channel from `bus.channels`.

Channel destruction takes effect at the channel's next tick boundary. Participants on other channels are unaffected.

### 5.4 Participant List Query

A participant registered on a channel may request the current participant list for that channel at any time, outside the tick loop. The request format is implementation-defined. The Arbiter must respond with a `MemberListResponse` (§3.9) reflecting the current state of `channel.participants` at the moment the request is processed.

The response is a point-in-time snapshot. Participants who join or leave the channel after the snapshot is taken will not appear in that response; the participant should rely on join/leave `BusEvents` (§5.1, §5.2) to maintain an up-to-date view, using the query only to reconcile after a delivery gap or on demand.

The Arbiter must not delay or defer a list query response to a tick boundary; it is answered immediately from current state. Concurrent reads during an in-progress tick are permitted and reflect whichever state is visible at the time of the read.

---

## 6. Observability

LTAP defines two conformance levels for observability:

**Base conformance** — the Arbiter **must** emit one structured record per participant per tick (including ineligible participants, which receive an implicit default rather than a bid):

```json
{
  "tick":              int,
  "channel_id":        "ChannelId",
  "participant":       "ParticipantId",
  "raw_priority":      float,
  "weighted_priority": float,
  "want_to_send":      bool,
  "intent":            "IntentTag",
  "won":               bool,
  "timed_out":         bool,
  "ineligible":        bool
}
```

For participants that were **ineligible** (`ineligible_ticks > 0`), the Arbiter was not solicited and no bid exists. These records must use the following fixed defaults: `raw_priority: 0.0`, `weighted_priority: 0.0`, `want_to_send: false`, `intent: "pass"`, `timed_out: false`, `won: false`, `ineligible: true`.

For participants that **timed out** or whose bid was **substituted** (safe default applied), `raw_priority` and `weighted_priority` reflect the substituted values (0.0 / 0.0), not any partial response that may have arrived.

**Replayable conformance** — in addition to the Base record, the Arbiter **must** include:

```json
{
  "contenders":        ["ParticipantId", "..."],
  "rng_state":         "<implementation-defined representation>"
}
```

`contenders` lists all participants in the near-tie band from which the winner was drawn (§4.4). When the eligible bid set was empty and selection did not execute, `contenders` must be an empty list `[]` and `rng_state` must be omitted or explicitly `null`. When selection did execute, implementations must capture `rng_state` *before* calling `random.choice`; state captured after selection cannot be used to replay it. The format of `rng_state` and the RNG algorithm are implementation-defined; exact replay additionally requires recording which algorithm was used alongside the state, even though the algorithm itself is not mandated by this specification.

Both conformance levels provide the signal needed to detect the following failure modes:

| Failure Mode | Detection Signal |
|---|---|
| Participant starvation | `won == false` for all bids by one participant over N ticks |
| Monopoly | Single participant wins consecutive ticks beyond `COOLDOWN_TICKS` |
| Bus lockup | No winner selected for N consecutive ticks |
| Bid collapse | All participants submit `want_to_send: false` repeatedly |
| Slow bidder | `timed_out == true` for the same participant across multiple ticks |
| Transmission loop | Same participant wins repeatedly with `won == true` but no bus output |
| Forced ineligibility | `ineligible == true` for a participant across multiple ticks |

---

## 7. Protocol Invariants

Conforming implementations must preserve all of the following. Violation of any constitutes a non-conforming implementation.

1. **Arbiter neutrality.** The Arbiter must not be a participant. It must not submit bids, produce transmissions, or hold participant-influenced arbitration state beyond the protocol records explicitly defined here. It must not semantically interpret participant content beyond the structured fields this protocol defines.
2. **Participant exclusion from evaluation.** No participant may read, influence, or be given access to any other participant's bid before the tick's winner is announced and the tick concludes.
3. **Bid isolation.** The Arbiter must not include one participant's bid data, private state, or undelivered bus events in anything it sends to another participant.
4. **Bid before execution.** The Arbiter must not signal transmission rights to any participant before all bids for the current tick have been collected (or timed out) and evaluated.
5. **At most one winner per tick.** If no eligible bids exist, no transmission occurs. If eligible bids exist, exactly one winner is selected and signalled.
6. **Serial execution.** The Arbiter solicits bids in parallel but signals transmission rights serially to at most one winner per tick.
7. **Direct-address bias.** The direct-address priority bias (§4.3 step 2) is evaluated against the most recent `ParticipantTransmission` in `channel.log` only. Self-address is suppressed before this rule can apply.
8. **Failure and timeout default to silence.** Any bid timeout, validation failure, or transmission failure produces no bus event for that participant on that tick. Ineligibility is not imposed on failure unless `failure_streak >= MAX_CONSECUTIVE_FAILURES`.
9. **Log append precedes broadcast.** The Arbiter appends to `channel.log` before attempting any participant broadcast. Broadcast delivery failures do not affect the log.
10. **Membership changes at tick boundaries.** External join and removal requests take effect only between ticks of the relevant channel. Multiple requests queued at the same boundary are processed in receipt order; removals before registrations.
11. **Ineligible participants are not solicited.** A participant with `ineligible_ticks > 0` on a channel receives no BidRequest for that channel and cannot win a tick on that channel until ineligibility expires.
12. **Channel independence.** A participant's arbitration state (ineligible_ticks, failure_streak, last_acted_tick) on one channel has no effect on its state on any other channel. The Arbiter must not use a participant's status or history on channel A when making arbitration decisions on channel B.
13. **Bid generation uses constrained decoding.** A conforming participant must produce bid responses under a generation-time constraint against the Bid Schema (§3.3.1), satisfying its deployment's declared conformance tier (§4.2). The Arbiter retains its validation and substitution logic as defense-in-depth but must not rely on it as the primary correctness mechanism for structural bid validity.
14. **Bid non-expressiveness.** Bid fields — including `intent` — MUST NOT be used to encode arbitrary payloads, hidden data, or inter-participant context leakage. Implementations MUST ensure bids remain non-expressive and bounded to their protocol-defined semantics. Bids are a control plane, not a data plane.

---

## 8. Out of Scope

The following are application-layer concerns and are explicitly outside this specification:

- Participant roles, domains, or behavioral parameters
- Priority honesty enforcement; adversarial priority manipulation
- How participants generate their transmissions; the internal mechanism by which constrained decoding is implemented (tokenizer integration, FSM/grammar engine, inference library) for bid generation
- Participant context management, memory, or history retention
- Application context accompanying a BidRequest
- `intent_version` format, versioning scheme, or negotiation
- Output content format, length, or post-processing
- Maximum transmission content size (the outcome when a limit is exceeded is specified in §4.5; the limit value itself is implementation-defined)
- Channel creation, destruction, naming, or access control
- How participants discover available channels
- Client-side deconfliction when a participant holds transmission rights on multiple channels simultaneously (e.g. winning two channels' ticks in the same wall-clock window); this is an internal client concern
- Bus topology and inter-channel participant addressing
- Transport mechanism between participants and the Arbiter
- Arbiter fault tolerance, redundancy, or recovery procedures. **Note:** the Arbiter is a single point of failure for all channels it manages. A conforming Arbiter redundancy or failover protocol is a v2 target; deployments requiring high availability must implement their own Arbiter-level fault tolerance outside this specification.
- RNG algorithm, seed management, or reproducibility strategy
- Policy for deregistering repeatedly timing-out or unreachable participants

---

## 9. Framework Comparison

The table below contrasts LTAP's protocol-level mechanisms against Murder Mystery Agents (MMA) — the closest published precedent for bid-based speaker self-selection among LLM agents — and against the common default patterns in AutoGen, CrewAI, and LangGraph. The MMA column describes the mechanism as published; the right column describes typical framework configurations, and all three frameworks are extensible and can approximate some of these behaviours through custom code.

MMA (Nonomura & Mori, *Who Speaks Next? Multi-party AI Discussion Leveraging the Systematics of Turn-taking in Murder Mystery Games*, [arXiv:2412.04937](https://arxiv.org/abs/2412.04937), December 2024) predates LTAP. Each agent's `think()` step emits a speak/listen decision and an integer importance (0–9); a program, not an LLM, selects the highest. LTAP shares that core and differs in what it adds around it: parallel bounded solicitation, constrained non-expressive bids, declared rather than inferred addressing, multiplicative rather than override biasing, cooldown, silence as a legal outcome, and channel/observability semantics.

| Feature | LTAP Protocol | Murder Mystery Agents (MMA) | AutoGen / CrewAI / LangGraph |
|---|---|---|---|
| **Selection Logic** | Deterministic & stochastic: participants submit numeric priority bids (0.0–1.0); the Arbiter applies a deterministic weighting pipeline then selects uniformly from a near-tie band. | Programmatic: each agent's `think()` emits `speak`/`listen` and an integer importance (0–9); a non-LLM function picks the highest, ties broken at random. No weighting pipeline — raw maximum. | LLM-based: a coordinator model reasons in natural language about who should speak next, based on conversation context and role descriptions. |
| **Speaker Intent** | Native: each participant explicitly signals `want_to_send`, `priority`, and an `IntentTag` in Phase 2 Bid Collection — a structured, protocol-defined mechanism. | Native: `action` and `importance` are explicit per-agent outputs — but emitted alongside a free-text `thought` in a single unconstrained generation, and that thought is retained in the agent's own short-term memory. This is the free-generation-then-parse pattern §4.2 excludes. | Simulated: frameworks lack a dedicated "I want to speak" signal; intent must be embedded in prompts or implemented as custom routing logic. |
| **Concurrency** | Strictly serial per channel (Protocol Invariant 6): bids are collected in parallel, but transmission rights are granted to exactly one winner per tick. | Serial: `think()` runs sequentially across agents each turn; exactly one speaker per turn. | Variable: default behaviour is serial, but LangGraph supports parallel node execution and AutoGen supports concurrent sub-chats ("swarm" patterns). |
| **Backoff / Cooldown** | Automated: for the `COOLDOWN_TICKS` ticks after a win, the winner's bids are multiplicatively dampened (×`DAMPENING_FACTOR`, reference 0.3), preventing a participant from immediately dominating subsequent ticks without any coordinator intervention — while leaving it able to win when no one else contends. | None: nothing dampens a repeat speaker; the same agent may win consecutive turns on raw importance alone. | Manual: no built-in cooldown mechanism; preventing consecutive wins requires explicit prompt engineering (e.g. "do not speak twice in a row") or custom routing functions. |
| **Direct Addressing** | Protocol-level: if the most recent `ParticipantTransmission` named a participant in `addressed_to`, that participant's bid priority is multiplied by `ADDRESS_BIAS` (reference ×2.0) in the next weighting pass (§4.3). The addressee is declared by the speaker as a structured field; the Arbiter never infers it from content. | Inferred and mandatory: a separate LLM call (`detectDesignation()`) reads the utterance for an adjacency-pair first part and its addressee; the detected addressee is obligated to speak next, overriding self-selection. Equivalent to the hard floor LTAP removed after field deployment (§11.1). | Contextual: agents may reply when they see their name in conversation text, but this depends on LLM inference — there is no arithmetic priority boost enforced by the framework. |
| **No-one Wants to Speak** | Silence: a tick with no eligible bid produces no transmission (§4.4). Abstention is a legal, first-class outcome. | Floor retention: when every agent selects `listen`, the previous speaker speaks again — the conversation-analysis rule that an unselected next turn returns to the current speaker. | Coordinator always selects someone; a turn with no speaker is not a native outcome. |

---

## 10. Known Limitations

**Priority dishonesty.** LTAP v1.0 assumes participants report bid priority honestly (§1). A participant that always bids maximum priority (1.0) does not win every tick — cooldown dampening (§4.3) limits consecutive wins and stochastic tie-breaking (§4.4) introduces non-determinism among near-tied bids — but it does persistently inflate its selection probability at the expense of participants bidding honestly. The protocol provides no detection or penalty mechanism for this behaviour in the current version.

Tuning controls to detect and mitigate priority inflation and other misbehaving-participant patterns are planned for a near release. These mechanisms are currently experimental and in development.

---

## 11. Field Notes — Deployment Experience

This section records what running LTAP taught us that designing it did not. The deployments referenced are the reference SDK (in-process and Kafka transports) and a multi-agent room simulation in which LLM-backed personas converse and move between rooms, each room an LTAP channel. Normative changes motivated by these notes are already incorporated into the sections above; the notes preserve the evidence and the reasoning.

### 11.1 A hard direct-address floor creates self-sustaining address loops

**Deployed:** the original §4.3 step 2, `priority = max(priority, 0.95)`, applied after cooldown dampening.

**Observed:** in rooms where agents habitually addressed one another, conversation collapsed into persistent A→B→C→A loops. The floor erased the addressee's own priority signal — any willing addressee jumped to 0.95 regardless of its raw bid — and, because it was applied after dampening, it overrode the one mechanism that could have broken the cycle. The application attempted a client-side fix (a soft multiplicative bump in its own bid shaping), which silently failed: the arbiter's floor re-applied on top of whatever the client submitted, and the floor was not configurable.

**Changed:** §4.3 step 2 became multiplicative (`× ADDRESS_BIAS`), composing with dampening instead of overriding it, and the bias became a named §3.8 parameter. Two lessons generalise: **a floor is a command, a multiplier is a nudge** — floors erase the signal the protocol exists to collect; and **an unconfigurable default defeats downstream evidence** — the application knew the default was wrong and had no lever to act on it.

### 11.2 Stored cooldown counters drift; derive the window instead

**Deployed:** three artifacts, three cooldown mechanisms — the spec described a stored, decremented `cooldown` counter with a `+1` phase-ordering offset; the reference SDK implemented a hard post-win lockout via `ineligible_ticks`; the application computed dampening from elapsed ticks after its own stored counter was found to never decrement, permanently dampening anyone who had ever spoken.

**Observed:** each formulation had an independent bug or design drawback, and the divergence itself produced an emergent failure: with the SDK's lockout *and* the application's client-side dampening both active, winners were double-penalised — hard-locked for the window, then dampened again after it. Nobody designed that behaviour; it emerged from three codebases each patching the same concept locally.

**Changed:** cooldown is now *derived* — `channel.tick − last_acted_tick ≤ COOLDOWN_TICKS` — from a field the Participant record already carried. No stored counter, nothing to decrement, no offset to compensate for, nothing to drift. The general lesson: **when protocol state can be derived from an existing event timestamp, derive it.** Every stored counter is a standing invitation for implementations to disagree about when it changes.

### 11.3 One owner per mechanic

**Observed:** both failures above share a root cause: the same conceptual mechanic implemented at two layers. Client-side address bump × arbiter-side floor; client-side dampening × arbiter-side lockout. Multiplicative pipelines make double application quietly catastrophic — two reference-value dampeners stack to ×0.09, which falls below the eligibility threshold and turns a soft cooldown into a post-win mute.

**Guidance:** every weighting mechanic must have exactly one owner. Applications that shape bid priority client-side (which the protocol expressly permits — how a participant computes its priority is out of scope, §8) SHOULD disable the corresponding Arbiter parameter (`DAMPENING_FACTOR = 1.0`, `ADDRESS_BIAS = 1.0`) rather than tolerate stacking. The Arbiter's weighting pipeline is a *default* for participants that do no shaping of their own, not a mandatory second pass.

### 11.4 Client-side cooldown ownership is legitimate — and sometimes better

**Observed:** the room simulation's client-side cooldown is richer than the protocol's: it exempts leave-the-room bids (an agent that just spoke should still be able to walk out — dampening its exit vote trapped agents in talk-then-fail-to-leave loops) and keys off *speech*, not any transmission (an agent that just moved rooms shouldn't arrive conversationally dampened). The Arbiter cannot express either distinction: `intent` is advisory by design (§3.3), and `last_acted_tick` records any win.

**Guidance:** applications whose intents carry action semantics should own cooldown shaping client-side and set `DAMPENING_FACTOR = 1.0`. This is the intended division of labour, not a workaround: the protocol arbitrates *contention*; what a turn is *for* is application knowledge.

### 11.5 Two-model routing is the cost pattern for the bid phase

**Observed:** the bid phase costs one inference per eligible participant per tick — with full-context prefill, this dominates deployment cost long before output tokens matter. The room simulation routes bids to a small model and only the winner's generation to a large one; it also threshold-gates dynamic prompt content (drive state appears only past a threshold) so the prefix-cacheable portion of the bid prompt stays stable across quiet ticks.

**Guidance:** deployments SHOULD route bid generation to a smaller model than transmission generation, and SHOULD structure bid prompts for prefix-cache stability. The bid is a ~20-token structured emission under constrained decoding (§4.2); it rarely needs the transmission model's capability. Reducing solicitation itself (standing bids, event-driven re-bidding) is a candidate for a future protocol revision.

### 11.6 Observability earned its cost

**Observed:** every failure mode in this section was diagnosed from the §6 tick records — the address loops from consecutive `won` streaks with the floor visible in `weighted_priority`, the double-dampening from `weighted_priority` collapsing to ×0.09 of `raw_priority`, the never-decrementing counter from a participant whose dampening never expired. None required adding instrumentation after the fact.

**Guidance:** implement Base conformance from day one. The per-participant-per-tick record (`raw_priority`, `weighted_priority`, `won`, `ineligible`) is the protocol's flight recorder; the failure-mode table in §6 was written from hypothetical failures, and deployment confirmed the signals detect real composite ones too.

### 11.7 The strict constrained-decoding MUST was unsatisfiable in practice

**Deployed:** the original §4.2 requirement — every participant MUST use an Equivalent Mechanism, including property (3), no transient generation of discardable content.

**Observed:** none of the deployments could meet it. Participants ran on hosted-style APIs and local model servers whose structured-output modes constrain the *emitted* message, not internal sampling, and reasoning-capable models generate non-emitted thinking tokens by design — one deployment stripped `<think>` blocks post-generation, precisely the pattern the requirement forbade. The practical effect of an unmeetable MUST was not stricter implementations; it was that the requirement was ignored wholesale, taking its meetable parts (schema-constrained emission) down with it.

**Changed:** §4.2 now defines two participant conformance tiers. Tier S preserves the full guarantee for deployments that control their decoding loop and genuinely share model state; Tier H makes the achievable discipline normative for API-backed participants — schema-constrained emission plus a client-layer guarantee that discarded content never enters any context that influences future protocol behaviour. The general lesson: **a requirement nobody can meet protects nothing** — tier it to what each deployment class can attest, and make the attestation explicit.
