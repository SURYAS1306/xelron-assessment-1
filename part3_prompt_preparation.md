# Part 3 — Prompt Preparation Document

Selected PR: **[aio-libs/aiokafka #193 — *Added `seek_to_beginning` and `seek_to_end` API*](https://github.com/aio-libs/aiokafka/pull/193)**

---

## 3.1.1 Repository Context

`aiokafka` is the asyncio-native client library for Apache Kafka in Python. It
gives you two main classes — `AIOKafkaProducer` and `AIOKafkaConsumer` — that
behave like the well-known producer / consumer of the official Java Kafka
client but cooperate with the `asyncio` event loop instead of using blocking
sockets and threads. The whole project is written in pure Python with a small
Cython hot path for record framing and compression codecs.

The intended users are Python backend engineers building event-driven systems
that already speak `asyncio`: think FastAPI / aiohttp services that need to
react to Kafka topics, stream-processing micro-services, log shippers, or any
worker that wants to mix Kafka I/O with HTTP calls and database I/O in the
same event loop. Two extra constituencies are: data engineers who use the
async client from notebooks because it cooperates with Jupyter's loop, and
authors of higher-level frameworks (Faust, aiokafka-rpc, Kafka-Faust forks)
that build on top of `aiokafka`.

The problem domain is **distributed messaging**: producing and consuming
records from a Kafka cluster while keeping all the semantics the Kafka protocol
demands — consumer-group coordination, partition assignment, offset commits,
heartbeats, rebalances, idempotent / transactional producers, compressed
record batches, etc. Internally the library is layered: `AIOKafkaClient` owns
the connection pool and metadata refresh; `Coordinator` / `NoGroupCoordinator`
own group membership and partition assignment; `Fetcher` and `Sender` are
background coroutines that pump messages between the broker and the user-facing
queues. `AIOKafkaConsumer` is the thin user-facing wrapper around all of that.

Anything that touches *where the consumer reads from* — `subscribe`, `assign`,
`seek`, `seek_to_committed`, the `auto.offset.reset` policy — flows through a
shared `SubscriptionState` object, which is what makes this PR feasible as a
small change.

## 3.1.2 Pull Request Description

The PR adds two new public coroutine methods to `AIOKafkaConsumer`:
`seek_to_beginning(*partitions)` and `seek_to_end(*partitions)`. Both of them
mirror the names and semantics of the equivalents in the Java client and have
been requested in issue [#154](https://github.com/aio-libs/aiokafka/issues/154).
With no arguments they apply to every partition currently assigned to the
consumer; with one or more `TopicPartition` arguments they apply only to those.

Previously the only options for repositioning a consumer were:

- `seek(tp, offset)` — jump to a specific absolute offset, which the user has
  to know up-front.
- `seek_to_committed(*tps)` — jump back to the last offset the consumer group
  has committed for those partitions.

There was **no convenient way** to say "tail this topic from now on" or
"replay this topic from the very start" without first calling `beginning_offsets`
/ `end_offsets` to look the numbers up and then `seek()`-ing to them. The new
methods collapse that two-step dance into one call and, more importantly,
reuse the existing `OffsetResetStrategy.EARLIEST` / `LATEST` machinery that
the library already uses for `auto.offset.reset`, so the implementation does
not duplicate any protocol code.

The PR also tightens the error contract of the existing `seek_to_committed`
so all three sibling methods behave the same way:

- An unassigned partition now raises `IllegalStateError` (previously it was a
  bare `AssertionError`, which is harder to catch and inconsistent with the
  Java client).
- Passing something that is not a `TopicPartition` raises `TypeError`
  (previously this would have produced a less helpful `AttributeError` deep
  inside the subscription state).
- All three methods now `await coordinator.ensure_partitions_assigned()` first,
  which means it is safe to call them immediately after `subscribe()` — the
  call blocks until the consumer has finished joining its group.

## 3.1.3 Acceptance Criteria

The implementation is considered complete only when **all** of the following
hold:

1. ✓ When `await consumer.seek_to_beginning()` is called with no arguments on a
   consumer that has been started and assigned partitions, **every** assigned
   partition's fetch position is reset to its earliest available offset, and
   the next `await consumer.getone()` returns the message at that offset.
2. ✓ When `await consumer.seek_to_end()` is called with no arguments, every
   assigned partition is reset to its current high-water mark, and a pending
   `await consumer.getone()` does NOT return until a *new* message is produced
   after the seek (verifiable with a `task.done() is False` check).
3. ✓ When either method is called with one or more `TopicPartition` arguments,
   the reset applies **only** to those partitions; other assigned partitions
   keep their current position untouched.
4. ✓ When either method is called with a `TopicPartition` that is not currently
   assigned to the consumer, the call raises `aiokafka.errors.IllegalStateError`
   whose message contains the set of offending partitions, and **no fetch
   position is mutated** on any partition.
5. ✓ When either method (including the existing `seek_to_committed`) is called
   with an argument that is not a `TopicPartition` instance (e.g. an `int`,
   `str`, or `None`), the call raises `TypeError` *before* any coordinator or
   fetcher work is done.
6. ✓ When either method is called on a consumer that has been started but has
   not yet joined its group (e.g. called immediately after `subscribe()`), the
   coroutine awaits the `JoinGroup` / `SyncGroup` handshake first and then
   performs the reset — it must NOT raise `AssertionError("No partitions are
   currently assigned")` racily.
7. ✓ The existing public API — `seek`, `seek_to_committed`, `assign`,
   `subscribe`, `getone`, `getmany`, `position`, `commit`, `committed` — keeps
   its current behaviour. The only intended behaviour change for
   `seek_to_committed` is that an unassigned partition / wrong type now raises
   `IllegalStateError` / `TypeError` instead of `AssertionError`, and this is
   documented under `.. versionchanged::`.
8. ✓ Both methods are exposed in `AIOKafkaConsumer`'s public docstring,
   appear in the Sphinx-generated API docs under `consumer.rst`, and carry a
   `.. versionadded:: 0.3.0` directive.
9. ✓ At least four new tests are added: a positive test for
   `seek_to_beginning`, a positive test for `seek_to_end` (including the
   "still pending after seek-to-end, gets woken up by a new produce"
   behaviour), a negative test for unassigned-partition errors, and a negative
   test for type errors.
10. ✓ Patch coverage for the diff stays at or above the project floor (~96 %)
    and `make cov` reports no regression on the rest of the code base.

## 3.1.4 Edge Cases

The model implementing this PR must explicitly think about the following
situations:

1. **Empty / freshly-created partition** — `seek_to_beginning` and
   `seek_to_end` should both return offset `0` (or whatever the broker
   reports), and a subsequent `await consumer.getone()` should not crash; it
   should simply block waiting for the first produce.
2. **Partition that has been compacted / log-truncated** — the *earliest*
   offset may be much larger than `0`. The implementation must use the broker
   reply rather than hard-coding `0`. (This also interacts with PR #115 which
   teaches the fetch buffer to handle non-contiguous offsets.)
3. **Concurrent rebalance while the seek is in flight** — the consumer may
   lose a partition between the time the user passes it in and the time the
   `ListOffsets` reply comes back. The implementation should either re-check
   assignment after the `await`, or rely on the `SubscriptionState` to reject
   the reset gracefully without leaving the position in a half-updated state.
4. **Mixing per-partition seeks with no-argument calls** — e.g. the user calls
   `seek_to_end(tp_a)` and then `seek_to_beginning()` (no args). The second
   call must apply to **every** currently assigned partition, including
   `tp_a`, overriding the earlier seek.
5. **No-group consumer (`group_id=None`)** — `ensure_partitions_assigned()`
   must be a no-op for `NoGroupCoordinator`, because there is no
   `JoinGroup` handshake. Calling `seek_to_beginning` / `seek_to_end`
   immediately after `assign()` must work without a deadlock.
6. **Calling before `await consumer.start()`** — should fail fast with the
   same `ConsumerStoppedError` / `IllegalOperation` that the rest of the API
   raises, not with an obscure attribute error.
7. **Performance** — the implementation must not issue a separate
   `ListOffsets` request per partition; it must batch them through the
   existing `Fetcher.update_fetch_positions` path, the same way
   `auto.offset.reset` already does.

## 3.1.5 Initial Prompt

> You are an experienced Python contributor working on the
> [`aio-libs/aiokafka`](https://github.com/aio-libs/aiokafka) repository at
> commit immediately before PR #193. The repository is a pure-Python /
> `asyncio` client for Apache Kafka organised around four main modules
> (`aiokafka/client.py`, `aiokafka/consumer.py`, `aiokafka/group_coordinator.py`,
> `aiokafka/fetcher.py`) and a `SubscriptionState` object that tracks each
> partition's fetch position and offset-reset strategy.
>
> Your task is to **add two new public coroutine methods on
> `AIOKafkaConsumer`**: `seek_to_beginning(*partitions)` and
> `seek_to_end(*partitions)`. Both methods must mirror the semantics of the
> equivalents in the official Java client: with no arguments they reposition
> every currently assigned partition, with one or more `TopicPartition`
> arguments they reposition only those.
>
> Implementation requirements
>
> 1. Both methods must reuse the existing `OffsetResetStrategy.EARLIEST` /
>    `OffsetResetStrategy.LATEST` machinery — call
>    `self._subscription.need_offset_reset(tp, OffsetResetStrategy.EARLIEST)`
>    (or `LATEST`) for each target partition, then
>    `await self._fetcher.update_fetch_positions(partitions)`. Do **not**
>    write a new `ListOffsets` request path.
> 2. Validate inputs up-front: raise `TypeError` if any positional argument is
>    not a `TopicPartition`. Raise the new `IllegalStateError` (re-exported
>    from `kafka.errors`) when a passed partition is not in the current
>    assignment; the error message must contain the offending set.
> 3. The first thing each method does (after argument validation) is
>    `await self._coordinator.ensure_partitions_assigned()`. Add that helper
>    to `BaseCoordinator` as a no-op and, on the real `GroupCoordinator`,
>    alias it to the existing `ensure_active_group()`. This lets the user
>    call the new methods immediately after `subscribe()` without races.
> 4. Refactor the existing `seek_to_committed` to use the same five-step
>    skeleton (type-check → ensure-assigned → default to all assigned →
>    `IllegalStateError` for unknowns → run the reset). Replace its previous
>    `AssertionError` paths with `IllegalStateError` / `TypeError`. Document
>    the change with `.. versionchanged:: 0.3.0`.
> 5. Re-export `IllegalStateError` from `aiokafka.errors`.
> 6. Add `.. versionadded:: 0.3.0` directives on the two new methods.
>
> Acceptance criteria
>
> Your change must satisfy every numbered item in section 3.1.3 of this
> document — in particular: a no-argument call repositions every assigned
> partition; a per-TP call repositions only those; an unassigned-TP call
> raises `IllegalStateError` and mutates **no** position; a non-`TopicPartition`
> argument raises `TypeError`; calling immediately after `subscribe()` must
> not race the rebalance.
>
> Edge cases to consider
>
> Handle the edge cases listed in section 3.1.4 of this document. In
> particular: compacted topics (earliest offset is not `0`); rebalances
> happening while the seek is in flight; no-group consumers
> (`NoGroupCoordinator` path); mixing per-TP and no-argument calls; never
> issuing one `ListOffsets` request per partition (must batch through
> `update_fetch_positions`).
>
> Testing requirements
>
> Add at least four tests under `tests/test_consumer.py`:
>
> - `test_consumer_seek_to_beginning`: produce 3 messages, consume one, call
>   `seek_to_beginning`, assert the next consumed message is the first one
>   again; then call `seek_to_beginning(tp)` and re-assert. Also assert
>   `position(tp) == start_position + 1` after the second consume.
> - `test_consumer_seek_to_end`: produce 3 messages, call `seek_to_end`,
>   create a `getone()` task and assert it is **not** done after 100 ms,
>   then produce more messages and assert the task resolves with the first
>   of the new messages. Repeat with a per-TP call.
> - `test_consumer_seek_on_unassigned`: assign `tp0` only, call each of the
>   three seek methods with `tp1`, assert `IllegalStateError` every time.
> - `test_consumer_seek_type_errors`: call each seek method with an `int`
>   argument, assert `TypeError`.
>
> Update `docs/consumer.rst` (or the equivalent Sphinx file) so the two new
> methods are listed in the public API table.
>
> Keep the diff focused: only `aiokafka/consumer.py`,
> `aiokafka/errors.py`, `aiokafka/group_coordinator.py`,
> `tests/test_consumer.py`, and the Sphinx file should change. Do not change
> the producer, the codec layer, or the Cython extensions. Run `make cov` —
> patch coverage must stay at or above 96 %.
