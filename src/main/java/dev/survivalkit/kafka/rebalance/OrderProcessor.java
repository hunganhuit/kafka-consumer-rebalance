package dev.survivalkit.kafka.rebalance;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Stands in for "real work" (write to DB, call a downstream service).
 *
 * <p>It records two things that let a test prove correctness across a rebalance:
 * <ul>
 *   <li>{@code processedCount} — how many times {@link #process} ran in total, so
 *       we can see duplicate processing.</li>
 *   <li>{@code distinctOrderIds} — the set of unique orders we handled, so we can
 *       see whether any order was lost.</li>
 * </ul>
 *
 * <p>The artificial delay simulates work that takes long enough for a rebalance
 * to interrupt an in-flight batch — which is exactly when naive consumers lose
 * or double-process data.
 */
public class OrderProcessor {
    private final AtomicInteger processedCount = new AtomicInteger();
    private final Set<String> distinctOrderIds = ConcurrentHashMap.newKeySet();
    private final long workMillis;
    public OrderProcessor(long workMillis) {
        this.workMillis = workMillis;
    }
    public void process(OrderEvent event) {
        if (workMillis > 0) {
            sleep(workMillis);
        }
        processedCount.incrementAndGet();
        distinctOrderIds.add(event.orderId());
    }
    public int processedCount() {
        return processedCount.get();
    }
    public Set<String> distinctOrderIds() {
        return Set.copyOf(distinctOrderIds);
    }
    public int distinctCount() {
        return distinctOrderIds.size();
    }
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while processing", e);
        }
    }
}
