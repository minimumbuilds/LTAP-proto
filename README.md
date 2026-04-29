# LLM Shared-Bus Turn Allocation Protocol (LTAP)
### Technical Specification — Request for Proposal — Version 1.0 Draft

---

## 1. Introduction

This document specifies a **tick-based contention protocol** for arbitrating transmission rights among a set of LLM participants sharing a common communication bus. Each tick executes in two logical phases: a **bid phase** (Phases 1–4), in which all participants declare intent and the Arbiter selects a winner without producing any output, and an **execution phase** (Phases 5–6), in which the winner transmits and the result is broadcast. The protocol prevents concurrent transmission and response flooding without imposing any constraint on participant role, domain, or output content.

The protocol is intentionally analogous in scope to a media-access control layer. Participant identity, purpose, output semantics, and internal context management are out of scope. The protocol defines only: how participants declare transmission intent, how that contention is resolved, and how the result is broadcast to all bus members.

**Arbiter separation is a foundational requirement of this protocol.** A participant that evaluates bids has a direct incentive to manipulate contention in its own favour. Bid evaluation must therefore be performed by a neutral process that is architecturally separate from the participant set, holds no generative role, and submits no bids of its own. This process is called the Arbiter. All phases of the tick loop are owned and executed exclusively by the Arbiter.

**Cooperative assumption.** LTAP assumes participants report priority honestly. The protocol provides no mechanism to detect or penalise a participant that always bids 1.0. Deployments involving adversarial or incentive-aware participants must implement application-layer mechanisms — reputation scoring, transmission budgets, quotas, or similar — outside the scope of this specification.

---

## 2. Definitions

| Term | Definition |
|---|---|
| `Participant` | An LLM-backed agent registered on one or more channels. Participants bid and transmit; they do not evaluate. |
| `Arbiter` | The neutral process that owns the tick loop for every channel, issues bid requests, evaluates contention, selects the winner, signals transmission rights, and broadcasts events. The Arbiter is not a participant and does not produce transmissions of its own. |
| `Channel` | A discrete broadcast domain managed independently by the Arbiter. Each channel has its own participant set, log, and tick counter. A participant may be registered on multiple channels simultaneously; its arbitration state (cooldown, ineligibility, failure streak) is tracked separately per channel. |
| `Bus` | The aggregate passive state managed by the Arbiter: the collection of all active channels. |
| `Tick` | One indivisible arbitration cycle within a single channel, driven by the Arbiter. Tick semantics are logical, not wall-clock. Channels tick independently. |
| `Bid phase` | Phases 1–4 of a tick: cooldown decrement, bid collection, bid weighting, and winner selection. No participant output is produced. |
| `Execution phase` | Phases 5–6 of a tick: winner transmission and event broadcast. |
| `BidRequest` | A message sent by the Arbiter to a participant to open the bid phase of a tick on a specific channel. |
| `Bid` | A structured intent declaration returned by a participant in response to a BidRequest. Bids are opaque to all other participants until the tick concludes. |
| `TransmissionResponse` | The message returned by the winning participant when signalled, carrying its output content and optional addressee. |
| `LogEntry` | A record appended to a channel's log; either a `ParticipantTransmission` or a `SystemEvent`. |
| `Bus Event` | A structured message broadcast by the Arbiter to participants after each tick, carrying a transmission or an infrastructure notification, scoped to the channel on which the event occurred. |
| `Cooldown` | A per-participant, per-channel backoff counter, maintained by the Arbiter, that suppresses bid priority for a fixed number of ticks after a participant transmits on that channel. |
| `Direct address` | A transmission explicitly directed at a named participant. Triggers a strong priority bias for that participant on every subsequent tick of the same channel until another `ParticipantTransmission` on that channel supersedes it as the most recent log entry. |

---

## 3. Data Model

### 3.1 Participant

```
Participant {
  id:               ParticipantId
  cooldown:         int    // maintained by the Arbiter; see §3.8 for semantics
  ineligible_ticks: int    // ticks remaining before participant may bid again; Arbiter-maintained
  failure_streak:   int    // consecutive transmission failures; maintained by the Arbiter
  last_acted_tick:  int    // maintained by the Arbiter
}
```

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

`addressed_to` is absent from the Bid. Who a participant intends to address is a transmission-time decision with no bearing on contention evaluation; it is declared in the TransmissionResponse (§3.4).

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
}
```

`is_self` lets a participant distinguish its own winning transmission from those of others without the Arbiter needing visibility into the participant's internal context.

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
  COOLDOWN_TICKS:             int     // dampened-bid count after transmitting; reference: 2
  BID_TIMEOUT:                float   // seconds to wait for a bid response; reference: 5.0
  TRANSMISSION_TIMEOUT:       float   // seconds to wait for a TransmissionResponse; reference: 60.0
  MAX_CONSECUTIVE_FAILURES:   int     // transmission failures before ineligibility; reference: 2
}
```

**Parameter floor values.** `COOLDOWN_TICKS = 0` effectively disables cooldown dampening: the counter is set to 1 post-transmission but decrements to 0 before weighting, so no bids are dampened. `MAX_CONSECUTIVE_FAILURES = 0` means every transmission failure immediately imposes ineligibility, since `failure_streak >= 0` is always true. Both are valid configurations for their respective disabling or maximum-strictness effects. Negative values are not permitted.

**Cooldown counter semantics.** `COOLDOWN_TICKS = N` means the participant's **next N bids** will be dampened. Because Phase 1 decrements the counter *before* Phase 3 applies the dampener, the Arbiter sets `cooldown = COOLDOWN_TICKS + 1` in Phase 6 so that the first post-decrement value is `COOLDOWN_TICKS` and the dampener fires for exactly N consecutive bids.

**Ineligibility counter semantics.** The same offset applies: the Arbiter sets `ineligible_ticks = COOLDOWN_TICKS + 1` when imposing ineligibility, producing exactly `COOLDOWN_TICKS` ticks during which the participant is not solicited.

The Arbiter's mutable runtime state is limited to the Bus and per-participant cooldown, ineligibility, and failure-streak counters. The Arbiter must hold no participant-influenced *arbitration state* beyond the protocol records explicitly defined here, and must not semantically interpret participant content beyond the structured fields this protocol defines (`addressed_to`).

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
    Phase 1:  Decrement counters            (channel.cooldown and channel.ineligible_ticks for all participants)
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

The Arbiter decrements every participant's `cooldown` and `ineligible_ticks` by one, each floored at zero, for the channel being ticked. This phase runs unconditionally regardless of channel occupancy. When `channel.participants` is empty the tick still completes normally — no bids are collected, no winner is selected, and `channel.tick` advances at the Boundary step. This preserves consistent tick numbering regardless of occupancy.

### 4.2 Phase 2 — Bid Collection

The Arbiter issues a `BidRequest` to every **eligible** participant in parallel and waits up to `BID_TIMEOUT` seconds for each response. A participant is eligible for solicitation when `ineligible_ticks == 0`. Ineligible participants are not solicited; they receive an implicit safe default for the tick and may become eligible again once `ineligible_ticks` reaches zero.

How each participant generates its bid is outside the protocol's scope.

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

| Field | Invalid condition | Resolution |
|---|---|---|
| `priority` | Non-finite (`NaN`, `Infinity`) | Substitute full safe default |
| `priority` | Outside `[0.0, 1.0]` but finite | Clamp to `[0.0, 1.0]` |
| `want_to_send` | Not a boolean | Substitute full safe default |
| `intent` | Not a recognised `IntentTag` | Substitute `intent: "pass"`; preserve `want_to_send` and `priority` |

An unrecognised `intent` is treated as advisory-unknown: the Arbiter substitutes `"pass"` as the intent label but preserves `want_to_send` and `priority` unchanged. Because `intent` is advisory with no effect on contention arithmetic (§3.3), a version-skew mismatch on this field does not silence an otherwise valid bid.

A timed-out or defaulted participant remains registered and eligible, and may bid normally in subsequent ticks. Repeated timeouts may be used as grounds for deregistration (§5.2) under implementation-defined policy.

Bids are held privately by the Arbiter and are never disclosed to other participants.

### 4.3 Phase 3 — Bid Weighting

The Arbiter post-processes raw priority values through a deterministic pipeline. No external calls are made. Modifiers are applied in the order listed.

| Step | Operation | Condition |
|---|---|---|
| 1 | `priority × 0.3` | `participant.cooldown > 0` |
| 2 | `priority = max(priority, 0.95)` | Most recent `ParticipantTransmission` in `channel.log` has `addressed_to` == this participant |
| 3 | `priority = clamp(priority, 0.0, 1.0)` | Always; applied last |

**Rationale — step 1 (cooldown dampening).** Prevents monopoly without hard-locking turns. Multiplicative, not binary: a participant under cooldown may still win if its raw priority is high enough.

**Rationale — step 2 (direct-address bias).** Gives a strong priority bias to a participant that was explicitly addressed in the most recent participant transmission on this channel. Evaluated after cooldown dampening; system events never displace it. Self-address is suppressed at TransmissionResponse validation (§4.5), preventing a sender from engineering its own bias. This is a bias, not a guarantee: the participant must still return `want_to_send: true`, and another participant bidding near 0.95 may win the stochastic tie-break.

**Ping-pong tradeoff.** When two participants consistently address each other, each alternately holds the 0.95 floor, producing near-guaranteed turn alternation. This is the intended behaviour for directed conversation threading, but it significantly weakens the fairness properties described in §4.4 for any third participant. Deployments where this is undesirable should apply an application-layer cap on the number of consecutive directly-addressed wins, or suppress the bias after N consecutive applications.

A bid is eligible only when `want_to_send == true` and weighted `priority > 0.1`.

### 4.4 Phase 4 — Winner Selection

The Arbiter selects the winner from the weighted bid set:

```
eligible   = [b for b in bids if b.want_to_send and b.priority > 0.1]
if eligible is empty → no transmission this tick; advance to Phase 6

top        = max(b.priority for b in eligible)
contenders = [b for b in eligible if b.priority >= top - 0.05]
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

**Broadcast.** The Arbiter then attempts to deliver a `BusEvent` of type `"transmission"` (with `channel_id` set) to every participant in the channel (`is_self: true` for the sender, `is_self: false` for all others). Broadcast is best-effort: delivery failures to individual participants are independent, do not affect delivery to others, do not undo the log append, and do not affect the winner's cooldown. A participant that fails to receive a BusEvent remains registered on the channel; repeated delivery failures may be grounds for deregistration under implementation-defined policy.

The Arbiter then sets, within the channel's participant record for the winner:
- `winner.last_acted_tick = channel.tick`
- `winner.cooldown = COOLDOWN_TICKS + 1`
- `winner.failure_streak = 0`

If Phase 5 produced no valid transmission, the Arbiter still advances `channel.tick` but appends no entry and broadcasts no event.

**Infrastructure events.** The Arbiter broadcasts a `BusEvent` of type `"system"` (with `channel_id` set) to all participants on the channel on every join or leave. This broadcast is unconditional and subject to the same best-effort delivery rules as transmission events. System events never trigger winner selection and never set cooldowns.

---

## 5. Membership

Membership is scoped to a channel. A participant registers on, and deregisters from, a specific channel. A participant may be registered on multiple channels simultaneously; its arbitration state on each channel is entirely independent. Membership changes take effect at a channel's tick boundary — never mid-tick. External requests received while that channel's tick is in progress are queued and applied in receipt order when the tick completes.

### 5.1 Registration

A participant registers on a specific channel. The Arbiter:

1. **Duplicate check.** If the `ParticipantId` is already present in `channel.participants` for the target channel, the Arbiter must reject the registration. The existing participant's record and counters are unchanged. The rejection response format is implementation-defined. The same `ParticipantId` may be registered on other channels without conflict.
2. Adds the participant to `channel.participants` with `cooldown = 0`, `ineligible_ticks = 0`, and `failure_streak = 0`.
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
7. **Direct-address bias applied after dampening.** The direct-address priority bias (§4.3 step 2) is evaluated after cooldown dampening, against the most recent `ParticipantTransmission` in `channel.log` only. Self-address is suppressed before this rule can apply.
8. **Failure and timeout default to silence.** Any bid timeout, validation failure, or transmission failure produces no bus event for that participant on that tick. Ineligibility is not imposed on failure unless `failure_streak >= MAX_CONSECUTIVE_FAILURES`.
9. **Log append precedes broadcast.** The Arbiter appends to `channel.log` before attempting any participant broadcast. Broadcast delivery failures do not affect the log.
10. **Membership changes at tick boundaries.** External join and removal requests take effect only between ticks of the relevant channel. Multiple requests queued at the same boundary are processed in receipt order; removals before registrations.
11. **Ineligible participants are not solicited.** A participant with `ineligible_ticks > 0` on a channel receives no BidRequest for that channel and cannot win a tick on that channel until ineligibility expires.
12. **Channel independence.** A participant's arbitration state (cooldown, ineligible_ticks, failure_streak, last_acted_tick) on one channel has no effect on its state on any other channel. The Arbiter must not use a participant's status or history on channel A when making arbitration decisions on channel B.

---

## 8. Out of Scope

The following are application-layer concerns and are explicitly outside this specification:

- Participant roles, domains, or behavioral parameters
- Priority honesty enforcement; adversarial priority manipulation
- How participants generate their bids or transmissions
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
