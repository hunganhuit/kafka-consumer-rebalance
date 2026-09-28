# Kafka Consumer Rebalance — The Production Guide

Your Kafka consumer works perfectly in dev. Then you scale to two pods, or a pod
restarts during a deploy, and suddenly orders go missing — or get processed
twice. Nobody changed the code. Welcome to rebalances.

This is a **free, runnable chapter** from the
[Production Backend Survival Kit](#-want-29-more-production-problems). Clone it,
run the test, read the code.

```bash
git clone <this-repo>
cd kafka-consumer-rebalance
mvn test        # needs a running Docker daemon (Testcontainers spins up Kafka)
```

Using Rancher Desktop / Colima instead of Docker Desktop? Point Testcontainers
at the socket first:

```bash
export DOCKER_HOST=unix://$HOME/.rd/docker.sock          # Rancher Desktop
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

---

## 1. Problem

When consumers join or leave a consumer group (scaling, deploys, crashes, or a
slow consumer being kicked out), Kafka **rebalances** — it reassigns partitions
across the surviving consumers. If your commit strategy is not tied to your
processing, a rebalance will either **lose** in-flight messages or **reprocess**
them.

## 2. Symptoms

- Records that were clearly produced never show up as processed (silent data loss).
- The same record is processed twice — duplicate emails, double charges, doubled counters.
- Symptoms appear only under load, during deploys, or when scaling pods.
- Logs show `Attempt to heartbeat failed... group is rebalancing`.

## 3. Root Cause

`enable.auto.commit=true` commits offsets on a background timer
(`auto.commit.interval.ms`), independently of whether processing actually
finished.

- Timer commits offset 200 while you are still on record 150 → rebalance → new
  owner starts at 200 → **records 150–199 lost**.
- Or you finished the records but the timer has not fired → rebalance →
  **new owner replays them**.

The commit is decoupled from the unit of work. That is the entire bug.

## 4. Why the naive implementation fails

Auto-commit runs on a timer with no idea where your processing loop is. Under a
single consumer you never notice — there is no rebalance. Add a second consumer
and the gap becomes data loss. See
[`BadRebalanceConsumer`](src/main/java/dev/survivalkit/kafka/rebalance/BadRebalanceConsumer.java).

## 5. Bad Implementation

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
// ...
while (running) {
    var records = consumer.poll(Duration.ofMillis(200));
    for (var record : records) {
        // Auto-commit may have ALREADY committed past this record.
        processor.process(new OrderEvent(record.value()));
    }
}
```

## 6. Production Implementation

Two changes:

1. **Manual commit tied to the unit of work** — `enable.auto.commit=false`, then
   `commitSync` only *after* the whole batch is processed.
2. **A `ConsumerRebalanceListener` that commits before losing a partition** —
   `onPartitionsRevoked` runs on your thread *before* reassignment, the last
   chance to commit finished work.

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (!pendingOffsets.isEmpty()) {
            consumer.commitSync(pendingOffsets);   // flush before revoke
            pendingOffsets.clear();
        }
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
});

while (running) {
    var records = consumer.poll(Duration.ofMillis(200));
    for (var record : records) {
        processor.process(new OrderEvent(record.value()));
        pendingOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1));
    }
    consumer.commitSync(pendingOffsets);   // commit only when batch is done
    pendingOffsets.clear();
}
```

Full source: [`ProductionRebalanceConsumer`](src/main/java/dev/survivalkit/kafka/rebalance/ProductionRebalanceConsumer.java).

**This gives you at-least-once, not exactly-once.** If you crash between
processing and commit, the record is replayed — which is why making processing
idempotent (a separate chapter in the full kit) matters. Correctness here means
**no loss**; duplicates are handled downstream.

## 7. Configuration

| Setting | Bad | Production | Why |
|---|---|---|---|
| `enable.auto.commit` | `true` | `false` | Take control of when offsets commit |
| `max.poll.records` | default (500) | tuned (e.g. 20) | Smaller batches = less replay on rebalance |
| rebalance listener | none | `commitSync` on revoke | Flush finished work before losing the partition |

`partition.assignment.strategy=CooperativeStickyAssignor` makes rebalances
incremental (cheaper) but does not replace correct commits.

## 8. Working Example

- [`BadRebalanceConsumer`](src/main/java/dev/survivalkit/kafka/rebalance/BadRebalanceConsumer.java) — the fragile version.
- [`ProductionRebalanceConsumer`](src/main/java/dev/survivalkit/kafka/rebalance/ProductionRebalanceConsumer.java) — the survivable version.
- [`OrderProcessor`](src/main/java/dev/survivalkit/kafka/rebalance/OrderProcessor.java) — records processed count + distinct ids so the test can detect loss/duplication.

## 9. How to Test

```bash
mvn test
```

[`RebalanceConsumerTest`](src/test/java/dev/survivalkit/kafka/rebalance/RebalanceConsumerTest.java)
spins up a real broker (Testcontainers), produces 200 orders across 2 partitions,
starts one consumer, then joins a **second** consumer to the same group to force
a rebalance mid-flight. It asserts all 200 distinct orders were processed — **no
loss**. Requires a running Docker daemon.

## 10. Common Mistakes

- Leaving `enable.auto.commit=true` and assuming Kafka "handles it."
- Doing heavy work in `onPartitionsRevoked` — it runs inside `poll()` and blocks the rebalance.
- Forgetting the `+ 1`: committed offset is the **next** offset to read, not the last processed.
- Assuming manual commit gives exactly-once. It gives at-least-once; dedupe downstream.

## 11. Production Checklist

- [ ] `enable.auto.commit=false`.
- [ ] Commit only after the unit of work completes.
- [ ] `ConsumerRebalanceListener` commits pending offsets in `onPartitionsRevoked`.
- [ ] Committed offset = last processed offset **+ 1**.
- [ ] `max.poll.records` sized so a batch finishes within `max.poll.interval.ms`.
- [ ] Downstream processing is idempotent so at-least-once is safe.
- [ ] Final commit + `consumer.close()` on shutdown.

---

## 🚀 Want 29 more production problems?

This is 1 of 30 chapters. The full **Production Backend Survival Kit** covers the
real problems that page you at 2am across Kafka, Redis, PostgreSQL, Spring Boot,
and Kubernetes — every one with a bad implementation, a production
implementation, runnable code, and tests.

### **[Get the kit — $9, one-time. Lifetime access to V1.x updates.](REPLACE_WITH_LANDING_PAGE_URL)**

Stop debugging Spring Boot production problems from scratch.

---

## License

This free sample is released under the [MIT License](LICENSE) — use it, share it,
learn from it. The full paid kit is sold under a separate commercial license.
