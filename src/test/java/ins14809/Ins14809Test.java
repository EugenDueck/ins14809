package ins14809;

import lombok.Data;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.fail;

class Ins14809Test {

    private static final String leftTopic = "left-topic";
    private static final String rightTopic = "right-topic";

    @TempDir
    Path stateStoreDirectory;

    @Test
    public void test() throws InterruptedException, IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");

        List<String> leftStreamLog = new ArrayList<>();
        List<String> rightStreamLog = new ArrayList<>();
        List<String> rekeyedLeftStreamLog = new ArrayList<>();
        List<String> rekeyedRightStreamLog = new ArrayList<>();
        Map<String, List<String>> results = new ConcurrentHashMap<>();

        // Run a real kafka broker in a docker container during the test.
        // I could not work out how to reporduce this behaviour with the TestTopologyDriver
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))) {
            kafka.start();

            Integer numberOfPartitions = 7;
            createTopics(kafka, numberOfPartitions);

            // On each topic, generate 1000 messages as described below, with a 1 hour
            // timestamp jump between each message.
            //
            produce1000MessagesOnBothTopicsInOrderButSpaced1HourApart(kafka);

            // Just for my sanity, check that the messages are really monitonically increasing
            // in timestamp on each topic-partition.
            confirmTimestampsIncreaseMonotonicallyInEachPartitionOfBothTopics(kafka);

            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> leftStream = builder.stream(leftTopic);
            final KStream<String, String> rightStream = builder.stream(rightTopic);

            leftStream.peek((key, value) -> leftStreamLog.add(value));
            rightStream.peek((key, value) -> rightStreamLog.add(value));

            final KStream<String, String> rekeyedLeftStream = leftStream
                    .selectKey((k, v) -> v.substring(0, v.indexOf(":")));

            final KStream<String, String> rekeyedRightStream = rightStream
                    .selectKey((k, v) -> v.substring(0, v.indexOf(":")));

            rekeyedLeftStream.peek((key, value) -> rekeyedLeftStreamLog.add(key + " " + value));
            rekeyedRightStream.peek((key, value) -> rekeyedRightStreamLog.add(key + " " + value));

            JoinWindows joinWindow = JoinWindows.of(Duration.ofSeconds(5));

            // A big grace setting will 'fix' the output, but as I understand it having a very large
            // grace period would be costly, and I feel like it shouldn't be necessary as the messages
            // on the topic are in fact in order when we start.

            // joinWindow = joinWindow.grace(Duration.ofDays(365));

            final KStream<String, String> joined = rekeyedLeftStream.leftJoin(
                    rekeyedRightStream,
                    (left, right) -> left + "/" + right,
                    joinWindow
            );

            joined.foreach((key, value) -> {
                synchronized(results) {
                    results.putIfAbsent(key, new ArrayList<>());
                    results.get(key).add(value);
                }
            });

            final Properties streamsConfiguration = new Properties();
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getName());
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDirectory.toAbsolutePath().toString());

            try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)) {
                kafkaStreams.start();
                waitUntilResultsEntryCountIsStable(results, Duration.ofSeconds(20));
            }
        }

        System.out.println();
        System.out.println("leftStream: " + leftStreamLog);
        System.out.println("rightStream: " + rightStreamLog);
        System.out.println("rekeyedLeftStreamLog: " + rekeyedLeftStreamLog);
        System.out.println("rekeyedRightStreamLog: " + rekeyedRightStreamLog);

        System.out.println();
        System.out.println("# Actual results");
        System.out.println();
        System.out.println("We want to see every number X below end with an entry that says [X,left/X,right]");
        System.out.println("but in practice we often see only [X,left/null] meaning the data was not joined.");
        System.out.println("This seems to coincide with kafka streams writing...");
        System.out.println("`WARN org.apache.kafka.streams.state.internals.AbstractRocksDBSegmentedBytesStore - Skipping record for expired segment`");
        System.out.println("...to its logs, in spite of the fact that the source message timestamps were in order when");
        System.out.println("kafka streams got them.");
        System.out.println();

        results.keySet().stream()
                .map((key) -> String.format("%3d %s", Integer.valueOf(key), results.get(key))).sorted()
                .forEach((line) -> System.out.println(line));
    }

    private void createTopics(final KafkaContainer kafka, Integer numberOfPartitions) {
        try (Admin adminClient = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ))) {
            adminClient.createTopics(List.of(new NewTopic(leftTopic, numberOfPartitions, (short) 1)));
            adminClient.createTopics(List.of(new NewTopic(rightTopic, numberOfPartitions, (short) 1)));
        }
    }

    private void produce1000MessagesOnBothTopicsInOrderButSpaced1HourApart(final KafkaContainer kafka) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ))) {

            Long offsetBetweenEvents = Duration.ofHours(1).toMillis();
            for (int i = 0; i < 1000; i++) {
                producer.send(new ProducerRecord<>(leftTopic, null, i * offsetBetweenEvents, null, i + ":left"));
                producer.send(new ProducerRecord<>(rightTopic, null, i * offsetBetweenEvents, null, i + ":right"));
                producer.flush();
            }

            producer.flush();
        }
    }

    private void confirmTimestampsIncreaseMonotonicallyInEachPartitionOfBothTopics(final KafkaContainer kafka) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName() + "Checker",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ))) {
            consumer.subscribe(List.of(leftTopic, rightTopic));

            Map<TopicAndPartition, Long> latestTimestampSeenSoFar = new HashMap<>();

            ConsumerRecords<String, String> results;
            do {
                results = consumer.poll(Duration.ofSeconds(5));

                results.forEach((record) -> {
                    TopicAndPartition topicAndPartition = new TopicAndPartition(record.topic(), record.partition());
                    Long timestamp = record.timestamp();

                    latestTimestampSeenSoFar.putIfAbsent(topicAndPartition, -1l);

                    if (latestTimestampSeenSoFar.get(topicAndPartition) >= timestamp) {
                        fail("Saw timestamp " + timestamp + " on " + topicAndPartition +
                             " but we already saw " + latestTimestampSeenSoFar.get(topicAndPartition));
                    }

                    latestTimestampSeenSoFar.put(topicAndPartition, timestamp);
                });
            } while (results.count() > 0);
        }
    }

    @Data
    private class TopicAndPartition {
        private final String topic;
        private final Integer partition;
    }

    private void waitUntilResultsEntryCountIsStable(final Map<String, List<String>> results, final Duration stableForAtLeast) throws InterruptedException {
        long previousResultCount = countResultEntries(results);
        Instant lastResultCountChange = Instant.now();
        while (lastResultCountChange.plus(stableForAtLeast).compareTo(Instant.now()) > 0) {
            long newResultCount = countResultEntries(results);
            if (previousResultCount != newResultCount) {
                previousResultCount = newResultCount;
                lastResultCountChange = Instant.now();
            }

            Thread.sleep(1000);
        }
    }

    private long countResultEntries(final Map<String, List<String>> results) {
        return results.entrySet()
                .stream()
                .map((e) -> e.getValue())
                .flatMap((l) -> l.stream())
                .count();
    }
}