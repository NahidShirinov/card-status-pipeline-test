package com.example.cardstatus;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the real pipeline (external-service call -> Postgres write -> Kafka publish)
 * against real, disposable infrastructure (Testcontainers), not mocks of Kafka/DB -
 * only the external card-status service is faked, using the same WireMock mappings
 * as mock-service/.
 *
 * Bump RECORD_COUNT to simulate a large uploaded file and see where p95/p99 and
 * Kafka consumer lag start to grow non-linearly - that is the bottleneck stage.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class CardStatusPipelineLoadTest {

    private static final String[] MAPPING_FILES = {
            "00-forced-success.json", "01-forced-error.json", "02-forced-timeout.json",
            "10-cycle-success-1.json", "11-cycle-success-2.json", "12-cycle-success-3.json",
            "13-cycle-success-4.json", "14-cycle-error.json"
    };

    private static final String TOPIC = "card-status-events";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @Container
    static GenericContainer<?> wiremock = buildWiremockContainer();

    private static GenericContainer<?> buildWiremockContainer() {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("wiremock/wiremock:3.9.1"))
                .withExposedPorts(8080)
                .withCommand("--global-response-templating")
                .waitingFor(Wait.forHttp("/__admin/mappings").forStatusCode(200));
        for (String file : MAPPING_FILES) {
            container = container.withCopyFileToContainer(
                    MountableFile.forClasspathResource("wiremock/mappings/" + file),
                    "/home/wiremock/mappings/" + file);
        }
        return container;
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("external.card-status.base-url",
                () -> "http://" + wiremock.getHost() + ":" + wiremock.getMappedPort(8080));
    }

    @Autowired
    private CardStatusProcessingService processingService;

    @Test
    void processesUploadedFileWithinLatencyBudget() throws Exception {
        int recordCount = 500; // <- bump to 5_000 / 50_000 to simulate a large uploaded file
        int concurrency = 20;  // <- mirrors how many concurrent calls your real service makes to the external API

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<ProcessingResult>> futures = IntStream.range(0, recordCount)
                .mapToObj(i -> {
                    String cardNumber = "41111111111" + String.format("%05d", i % 100_000);
                    return pool.submit(() -> processingService.process(cardNumber, cardNumber, "BLOCKED"));
                })
                .collect(Collectors.toList());

        long start = System.nanoTime();
        List<ProcessingResult> results = new ArrayList<>();
        for (Future<ProcessingResult> f : futures) {
            results.add(f.get(60, TimeUnit.SECONDS));
        }
        long totalWallClockMs = (System.nanoTime() - start) / 1_000_000;
        pool.shutdown();

        printLatencyReport(results, totalWallClockMs);
        assertConsumerLagIsZeroAfterDraining();
    }

    private void printLatencyReport(List<ProcessingResult> results, long totalWallClockMs) {
        List<Long> totals = results.stream().map(ProcessingResult::totalMs).sorted().toList();
        List<Long> externalTimes = results.stream().map(ProcessingResult::externalCallMs).sorted().toList();
        long successCount = results.stream().filter(ProcessingResult::success).count();
        long failureCount = results.size() - successCount;

        System.out.println("=== Load test report ===");
        System.out.println("records: " + results.size());
        System.out.println("success: " + successCount + "  failure: " + failureCount);
        System.out.printf("wall-clock total: %d ms, throughput: %.1f records/sec%n",
                totalWallClockMs, results.size() * 1000.0 / totalWallClockMs);
        System.out.printf("per-record total latency   p50=%dms p95=%dms p99=%dms%n",
                percentile(totals, 50), percentile(totals, 95), percentile(totals, 99));
        System.out.printf("external-call latency only p50=%dms p95=%dms p99=%dms%n",
                percentile(externalTimes, 50), percentile(externalTimes, 95), percentile(externalTimes, 99));
    }

    private static long percentile(List<Long> sortedValues, int percentile) {
        if (sortedValues.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        return sortedValues.get(Math.max(0, Math.min(index, sortedValues.size() - 1)));
    }

    /**
     * Demonstrates how to measure real Kafka consumer lag programmatically:
     * drain the topic with a consumer group, then ask AdminClient for the
     * difference between each partition's latest offset and that group's
     * committed offset. In a real load test you would poll this DURING the
     * run (not just at the end) to see lag grow and shrink over time.
     */
    private void assertConsumerLagIsZeroAfterDraining() throws Exception {
        String groupId = "load-test-consumer";
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        int drained = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                drained += records.count();
                if (records.isEmpty() && drained > 0) break;
            }
            consumer.commitSync();
        }
        System.out.println("Kafka messages drained by test consumer: " + drained);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);

            Map<TopicPartition, OffsetSpec> latestSpecs = committed.keySet().stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(latestSpecs).all().get(10, TimeUnit.SECONDS);

            committed.forEach((tp, offsetMeta) -> {
                long end = endOffsets.get(tp).offset();
                long lag = end - offsetMeta.offset();
                System.out.println("partition " + tp.partition() + " consumer lag = " + lag);
                assertThat(lag).isEqualTo(0);
            });
        }
    }
}
