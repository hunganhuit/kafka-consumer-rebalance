package dev.survivalkit.kafka.rebalance;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The WRONG way. This is the code most people write first, and it looks fine
 * until a rebalance happens under load.
 *
 * <p><b>What is wrong:</b> {@code enable.auto.commit=true} with a short
 * {@code auto.commit.interval.ms}. Kafka commits offsets on a timer, on a thread
 * you do not control, <em>independently of whether you actually finished
 * processing those records</em>.
 *
 * <p><b>How it fails:</b>
 * <ul>
 *   <li><b>Message loss:</b> {@code poll()} returns records 100..199. The auto-commit
 *       timer fires and commits offset 200 while you are still processing record
 *       150. A rebalance moves the partition to another consumer, which starts at
 *       200. Records 150..199 are never processed. Gone.</li>
 *   <li><b>Duplicate processing:</b> the mirror image — you finish records but the
 *       commit has not fired yet when the rebalance hits, so the next owner
 *       reprocesses them.</li>
 * </ul>
 *
 * <p>The root problem is that the commit is decoupled from the unit of work.
 */
public class BadRebalanceConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BadRebalanceConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final OrderProcessor processor;
    private volatile boolean running = true;
    public BadRebalanceConsumer(
            Map<String, Object> baseProps, String topic, OrderProcessor processor) {
        Properties props = new Properties();
        props.putAll(baseProps);
        // The two lines that cause the bug.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.processor = processor;
    }
    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(topic));
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    // Auto-commit may have ALREADY committed past this record before
                    // we even run this line. There is no coordination.
                    processor.process(new OrderEvent(record.value()));
                }
            }
        } catch (Exception e) {
            log.error("bad consumer crashed", e);
        } finally {
            consumer.close();
        }
    }
    public void stop() {
        running = false;
    }
}
