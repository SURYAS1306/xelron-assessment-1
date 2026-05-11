# Part 2 — Pull Request Analysis

Repository chosen for this section: **[`aio-libs/aiokafka`](https://github.com/aio-libs/aiokafka)**.

After skimming all 10 PRs (#25, #115, #143, #193, #196, #201, #217, #232, #237, #1006),
I picked the two below because they are *self-contained*, have a clear before /
after behaviour, and the diffs are small enough that I can describe every change
in my own words without hand-waving over architecture I don't really know.

| # | PR | Why I picked it |
|---|---|---|
| A | [PR #193 — *Added `seek_to_beginning` and `seek_to_end` API*](https://github.com/aio-libs/aiokafka/pull/193) | Pure feature add that mirrors the well-known Java client API. Diff is ~200 lines and touches only 5 files. |
| B | [PR #115 — *Added fix to support compacted topics data, where some offsets can be skipped*](https://github.com/aio-libs/aiokafka/pull/115) | Concrete bug fix with a precise root cause (offset-equality check) and a clean fix (offset-≥ check + a new `drop_pending_message_set` flag). |

---

## PR A — #193 · `Added seek_to_beginning and seek_to_end API`

Link: <https://github.com/aio-libs/aiokafka/pull/193>
Author: `@tvoinarovskyi` · Merged `Jul 27 2017` · 207 additions / 21 deletions across 5 files.

### PR Summary (≈ 130 words)

Before this change `AIOKafkaConsumer` only let you reposition a partition with
two methods: `seek(tp, offset)` (jump to a specific absolute offset) and
`seek_to_committed(*tps)` (jump back to the last committed offset of a consumer
group). There was no convenient way to say *"start over from the beginning of
the log"* or *"skip everything that already happened and tail the topic"* —
something every other Kafka client (including the official Java one) offers as
`seekToBeginning()` / `seekToEnd()`. Issue [#154](https://github.com/aio-libs/aiokafka/issues/154)
asked for those two APIs. PR #193 adds them as `async` methods on the consumer,
delegates the actual reset to the existing `OffsetResetStrategy.EARLIEST` /
`LATEST` machinery, and also tightens the error-handling on the existing
`seek_to_committed` so all three sibling APIs behave consistently.

### Technical changes (files / components modified)

- `aiokafka/consumer.py`
  - Imports `OffsetResetStrategy` from `kafka.protocol.offset` and the new `IllegalStateError`.
  - Adds `async def seek_to_beginning(*partitions)`.
  - Adds `async def seek_to_end(*partitions)`.
  - Rewrites `seek_to_committed(*partitions)` so that an unassigned partition raises `IllegalStateError` instead of an `AssertionError`, and partitions that are not `TopicPartition` instances raise `TypeError`.
  - Cleans up `assign()`: the per-TP `UnknownTopicOrPartitionError` check is removed (the same check is enforced downstream by `set_topics`), and the order of `_on_change_subscription()` / `set_topics()` is fixed.
  - Refactors `seek()` docstring to mention that it does not respect autocommit.
- `aiokafka/errors.py`
  - Re-exports `IllegalStateError` from `kafka-python` in `__all__`.
- `aiokafka/group_coordinator.py`
  - Adds `ensure_partitions_assigned()` on the `BaseCoordinator`. In the `NoGroupCoordinator` it is a no-op; in the full `GroupCoordinator` it is aliased to the already-existing `ensure_active_group()` (which blocks until the consumer has finished its `JoinGroup` + `SyncGroup` handshake).
  - A small fix in `_heartbeat_task_routine` where `sleep_time` was not being reset before `continue`.
- `tests/test_consumer.py`
  - Four new tests: `test_consumer_seek_to_beginning`, `test_consumer_seek_to_end`, `test_consumer_seek_on_unassigned`, `test_consumer_seek_type_errors`.
- `tests/test_client.py`
  - Stabilises an existing flaky timing test by enlarging the sleeps (unrelated noise).

### Implementation approach (≈ 180 words)

The two new methods follow the same skeleton, which is the key design decision
of the PR — the author keeps both functions almost identical so the behaviour
is symmetric and easy to test:

1. Validate the arguments: every positional argument must be a `TopicPartition`,
   otherwise raise `TypeError`.
2. `await self._coordinator.ensure_partitions_assigned()`. This is the trick
   that makes the API safe to call right after `subscribe()` — the consumer is
   not even joined to its group yet at that point, so the coordinator coroutine
   blocks until `JoinGroup` / `SyncGroup` succeed and the partitions are
   actually assigned. For the no-group path it is a no-op.
3. If no partition is passed, default to *all* currently assigned partitions;
   otherwise verify that every passed TP is in the assigned set and raise
   `IllegalStateError` (with the offending set in the message) when it is not.
4. For each TP, call `self._subscription.need_offset_reset(tp, EARLIEST|LATEST)`.
   This sets a flag inside `SubscriptionState` which is the same flag that the
   normal "auto offset reset" logic uses, so all the existing fetch / reset
   plumbing is reused unchanged.
5. Finally `await self._fetcher.update_fetch_positions(partitions)` to do the
   actual `ListOffsets` request and seed the in-memory position. After the
   await returns, a subsequent `getone()` / `getmany()` will fetch starting at
   the earliest / latest offset.

`seek_to_committed` is reshaped to use the same 5-step skeleton so all three
methods share the validation and the new `IllegalStateError` semantics.

### Potential impact (≈ 90 words)

The change is **additive** for `seek_to_beginning` / `seek_to_end` — no caller
can be relying on these methods because they didn't exist. The **breaking part**
is `seek_to_committed`: callers that previously caught `AssertionError` will
no longer see one — they will see `IllegalStateError` or `TypeError`. This is
intentional and is documented under `.. versionchanged:: 0.3.0`. The `assign()`
cleanup removes a defensive `UnknownTopicOrPartitionError` raise; downstream
metadata refresh in `set_topics` is now the single point that decides whether a
topic-partition is valid. Consumers that subscribe to non-existent topics may
therefore see a slightly later / different error path.

---

## PR B — #115 · `Added fix to support compacted topics data, where some offsets can be skipped`

Link: <https://github.com/aio-libs/aiokafka/pull/115>
Author: `@tvoinarovskyi` · Merged `Mar 19 2017` · 165 additions / 33 deletions across 4 files.

### PR Summary (≈ 130 words)

In a Kafka **log-compacted** topic the broker is allowed to delete older records
that have been superseded by a newer record with the same key. The visible
effect on the consumer is that **offsets are non-contiguous** — you might
receive offset 160, then 162, then 167. Issue [#71](https://github.com/aio-libs/aiokafka/issues/71)
reported that `aiokafka` would silently drop these messages because its fetch
buffer required `msg.offset == position` — i.e. an exact match against the next
expected offset. As soon as the broker compacted the log, the consumer would
hit the first hole, refuse to advance, and effectively stall on that partition.
This PR replaces the strict equality with a "≥ position, then advance" rule
and adds a small piece of state (`drop_pending_message_set`) so the consumer
also behaves correctly when the user calls `seek()` while a fetch is in flight.

### Technical changes (files / components modified)

- `aiokafka/fetcher.py`
  - Renames `_check_assignment` to `check_assignment` (drops the leading underscore so the buffer can call it).
  - Rewrites `getone()`: instead of `if msg.offset == position` it now does:
    - if `msg.offset < position` → skip (this is a compressed message-set leftover, perfectly normal),
    - elif `msg.offset > position and drop_pending_message_set` → user has seeked, throw away the rest of the buffer,
    - else → accept the message and advance `position` to `msg.offset + 1` (not `position + 1`).
  - Rewrites `getall()` with the same three-branch logic and an early bail-out when the very first message is past the current position.
  - In `_proc_fetch_request`, when the fetch comes back with `fetch_offset == position`, clears the `drop_pending_message_set` flag (the in-flight reply is now consistent with where the user has seeked to).
  - Replaces a `# noqa` with `# pragma: no cover` on the generic "unexpected error" branch — purely cosmetic.
- `aiokafka/producer.py` — same `# noqa` → `# pragma: no cover` cleanup.
- `tests/test_consumer.py`
  - Adds a `count_fetch_requests` `contextmanager` so the new tests can assert how many fetch round-trips actually happened.
  - Splits `test_consumer_seek_forward` into `_getone` / `_getmany` variants and tightens the assertions (especially that a forward seek inside the buffered batch does NOT trigger a new fetch).
  - Strengthens `test_consumer_seek_backward` to also exercise `getmany`.
- `tests/test_fetcher.py`
  - Adds `test_compacted_topic_consumption`: builds a synthetic `FetchResponse` with offsets `160, 162, 167`, seeks the consumer to `155`, and asserts that all three messages are returned and `position` walks `161 → 163 → 168`.

### Implementation approach (≈ 190 words)

The PR isolates the bug in one place — the `FetchBuffer.getone` / `getall`
loops. The root cause is the assumption "the broker only ever returns the next
offset after my current `position`", which is true for *normal* topics but
false for *compacted* ones. The fix is conceptually trivial: when you peek at
the next message in the buffer you compare to `position` with `≥` instead of
`==`, and you advance `position` to `msg.offset + 1` so any *gap* between the
expected offset and the received offset is silently swallowed.

The subtle part is the second invariant the buffer has to uphold: *what happens
if the user calls `seek()` while a fetch is already in flight?* A naïve
implementation would still hand out the in-flight messages even though the user
explicitly asked to read from somewhere else. The PR fixes this by introducing
a per-partition `drop_pending_message_set` flag on `TopicPartitionState`. The
flag is set by `seek()` (and friends) and cleared inside `_proc_fetch_request`
the first time a response is received whose `fetch_offset` equals the new
`position`. While the flag is set, the buffer treats *any* `msg.offset >
position` as stale and clears itself. This keeps the two concerns
(`compacted-offsets`, `user-seek-during-fetch`) cleanly separated in the same
buffer.

### Potential impact (≈ 95 words)

This is an internal fix to the consumer fetch loop, so the public API does not
change. The behavioural impact is exactly what the issue asks for: consumers
that read from compacted topics no longer freeze on the first offset gap and
their `position` correctly tracks the highest delivered offset + 1. As a
secondary effect, calling `seek()` while a fetch is in flight is now safe — the
old code would have surfaced the stale records and silently advanced `position`
incorrectly. The `_check_assignment` rename is technically a private-API change
but no external caller can reasonably depend on it.
