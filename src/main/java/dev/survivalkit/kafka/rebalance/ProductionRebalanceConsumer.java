package dev.survivalkit.kafka.rebalance;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The RIGHT way. Two changes turn the fragile consumer into a survivable one.
 *
 * <p><b>1. Manual commit tied to the unit of work.</b>
 * {@code enable.auto.commit=false}. We call {@code commitSync} only <em>after</em>
 * every record in the batch has been fully processed. The commit now means what
 * we want it to mean: "everything up to here is done."
 *
 * <p><b>2. A {@link ConsumerRebalanceListener} that commits before losing a
 * partition.</b> When the group rebalances, {@code onPartitionsRevoked} runs on
 * this consumer's own thread <em>before</em> the partition is handed to someone
 * else. That is our last chance to commit the offsets of work we already
 * finished. Commit there and the new owner resumes at exactly the right place —
 * no gap, minimal overlap.
 *
 * <p><b>Why "at-least-once", not "exactly-once":</b> if we crash between finishing
 * a record and committing, the record is reprocessed after rebalance. That is
 * unavoidable with this model — which is precisely why the next chapter
 * (Idempotency) exists. Correctness here means <em>no loss</em>; duplicates are
 * handled downstream by making processing idempotent.
 */
public class ProductionRebalanceConsumer implements Runnable {
    private static final Logger log =
            LoggerFactory.getLogger(ProductionRebalanceConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final OrderProcessor processor;
    // Offsets we have fully processed but not yet committed. The rebalance
    // listener flushes these so the next owner does not replay them.
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
    // How many times this consumer lost partitions to a rebalance. Lets a test
    // prove a rebalance really happened mid-flight instead of assuming it.
    private final AtomicInteger revocations = new AtomicInteger();
    private volatile boolean running = true;
    public ProductionRebalanceConsumer(
            Map<String, Object> baseProps, String topic, OrderProcessor processor) {
        Properties props = new Properties();
        props.putAll(baseProps);
        // Commit is now OUR decision, not a background timer's.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.processor = processor;
    }
    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic), new CommitOnRevokeListener());
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    processor.process(new OrderEvent(record.value()));
                    // Track the NEXT offset to read for this partition (current + 1).
                    pendingOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                }
                // Only commit once the whole batch is genuinely done.
                consumer.commitSync(pendingOffsets);
                pendingOffsets.clear();
            }
        } catch (Exception e) {
            log.error("production consumer crashed", e);
        } finally {
            // Best-effort final commit, then leave the group cleanly.
            try {
                if (!pendingOffsets.isEmpty()) {
                    consumer.commitSync(pendingOffsets);
                }
            } finally {
                consumer.close();
            }
        }
    }
    public void stop() {
        running = false;
    }
    public int revocations() {
        return revocations.get();
    }
    /**
     * The critical piece. {@code onPartitionsRevoked} is invoked on the consumer
     * thread during {@code poll()} as part of the rebalance, before partitions are
     * reassigned. Committing here is what prevents both loss and needless replay.
     */
    private final class CommitOnRevokeListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty()) {
                revocations.incrementAndGet();
            }
            if (!pendingOffsets.isEmpty()) {
                log.info("rebalance: committing {} offsets before revoke", pendingOffsets.size());
                consumer.commitSync(pendingOffsets);
                pendingOffsets.clear();
            }
        }
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("rebalance: assigned {} partitions", partitions.size());
        }
    }
}
