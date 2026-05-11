# Part 4 — Technical Communication

## Scenario response

**Question from the reviewer:**
*"Why did you choose this specific PR over the others? What made it comprehensible to you, and what challenges do you anticipate in implementing it?"*

---

I picked **[aiokafka PR #193 — *Added `seek_to_beginning` and `seek_to_end`
API*](https://github.com/aio-libs/aiokafka/pull/193)** for three concrete
reasons. First, the PR has a *very small surface area*: ~200 lines across five
files, with two new methods and one refactor of a sibling method. Second, it
mirrors an API that already exists in the official Java Kafka client, so the
expected semantics — name, signature, behaviour around assigned vs unassigned
partitions — are not invented; they are a well-documented spec I can read in
the upstream `Consumer.java` javadoc and then port. Third, the change is
*purely additive on the public surface*, which makes it possible to write a
clear acceptance test that doesn't have to reason about ten interlocking
modules.

My technical background lines up reasonably well with this PR. Through my open-source contributions and GSoC work, I have spent time exploring larger backend-oriented codebases, understanding existing abstractions, and tracing request and state-management flows across multiple modules.

My GSoC project around authentication and RBAC integration in Uyuni also involved studying real authentication pipelines, role-management flows, connection handling, and integration strategies inside an existing production-scale system. That made it easier for me to follow how `aiokafka` coordinates partition assignment, fetch-position management, and offset-reset handling across the consumer, coordinator, and fetcher layers.

I was also more comfortable with this PR because it extends existing consumer-offset logic instead of introducing a completely separate subsystem. At the same time, I intentionally avoided PRs that heavily modified lower-level components like the Cython record implementation or authentication protocol internals because I wanted to choose a change that I could fully understand and reason about within the assessment timeline.

The challenges I anticipate are concurrency-related, not API-design ones.

1. **Races against rebalances.** Between argument validation and the
   `await fetcher.update_fetch_positions(partitions)` call, the consumer can
   be revoked and reassigned partitions. I would handle this by snapshotting
   `_subscription.assigned_partitions()` after `ensure_partitions_assigned()`
   and re-validating the requested set against that snapshot.
2. **`NoGroupCoordinator` path.** I have to remember to make
   `ensure_partitions_assigned()` a no-op there, otherwise tests with
   `group_id=None` will hang. I would catch this with a dedicated unit test.
3. **Test flakiness for `seek_to_end`.** "Wait 100 ms and check `task.done()`
   is `False`" is timing-sensitive on CI. I would mitigate by using a longer
   sleep on slow CI and by additionally asserting the post-condition
   (`position(tp) == high_water_mark`) which does not depend on timing.

If I got stuck, I would lean on the merged PR's tests as my contract and use
the upstream Java client's docstrings as the behavioural spec.

---

## Integrity Declaration

> I declare that all written content in this assessment is my own work, created
> without the use of AI language models or automated writing tools. All
> technical analysis and documentation reflects my personal understanding and
> has been written in my own words.
