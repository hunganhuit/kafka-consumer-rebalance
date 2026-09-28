package dev.survivalkit.kafka.rebalance;
import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
/**
 * Proves the production consumer processes every message across a rebalance,
 * with no loss. Runs against a real broker via Testcontainers.
 *
 * <p>The scenario: one consumer starts, work begins, then a second consumer joins
 * the same group. Kafka triggers a rebalance to split the partitions. A naive
 * auto-commit consumer would drop or replay records around that moment. We assert
 * that the production consumer handled all distinct orders.
 */
@Testcontainers
class RebalanceConsumerTest {
    private static final String TOPIC = "orders";
    private static final int PARTITIONS = 2;
    private static final int TOTAL_ORDERS = 200;
    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));
    private static ExecutorService pool;
    @BeforeAll
    static void setUp() throws Exception {
        pool = Executors.newCachedThreadPool();
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(
                    java.util.List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
    }
    @AfterAll
    static void tearDown() {
        pool.shutdownNow();
    }
    @Test
    void productionConsumerLosesNoMessagesAcrossRebalance() throws Exception {
        produceOrders(TOTAL_ORDERS);
        // 20ms per record -> ~4s to drain alone, long enough to interrupt.
        OrderProcessor processor = new OrderProcessor(20);
        Map<String, Object> baseProps = consumerProps("prod-group");
        // First consumer starts and begins draining the topic.
        ProductionRebalanceConsumer consumerA =
                new ProductionRebalanceConsumer(baseProps, TOPIC, processor);
        pool.submit(consumerA);
        // Wait until A is genuinely mid-flight: some work done, most still pending.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> processor.processedCount() >= 20);
        assertThat(processor.processedCount()).isLessThan(TOTAL_ORDERS);
        // Second consumer joins the SAME group -> forces a rebalance mid-flight.
        ProductionRebalanceConsumer consumerB =
                new ProductionRebalanceConsumer(baseProps, TOPIC, processor);
        pool.submit(consumerB);
        // Every distinct order must eventually be processed. No loss allowed.
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(processor.distinctCount()).isEqualTo(TOTAL_ORDERS));
        consumerA.stop();
        consumerB.stop();
        assertThat(processor.distinctOrderIds()).hasSize(TOTAL_ORDERS);
        // Prove the scenario actually happened: A lost partitions mid-flight.
        assertThat(consumerA.revocations()).isGreaterThanOrEqualTo(1);
    }
    private void produceOrders(int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String orderId = "order-" + i;
                producer.send(new ProducerRecord<>(TOPIC, orderId, orderId));
            }
            producer.flush();
        }
    }
    private Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        return props;
    }
}
