package ir.sahab.micrometer.kafka;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.*;

public class MicrometerKafkaMetricsReporterTest {

    public static final String TOPIC_NAME = "TestTopic";

    @ClassRule
    public static EmbeddedKafkaRule kafkaRule = new EmbeddedKafkaRule(1, true, TOPIC_NAME);

    @Before
    public void setUp() {
        // Add a simple registry implementation to use in tests.
        Metrics.globalRegistry.getRegistries().forEach(Metrics::removeRegistry);
        Metrics.globalRegistry.add(new SimpleMeterRegistry());
    }

    @Test
    public void testProducerMetrics() {

        HashMap<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, kafkaRule.getEmbeddedKafka().getBrokersAsString());
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(CLIENT_ID_CONFIG, "producer");
        configs.put(METRIC_REPORTER_CLASSES_CONFIG, MicrometerKafkaMetricsReporter.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        producer.send(new ProducerRecord<>(TOPIC_NAME, "Key", "value"));
        producer.flush();

        int total_records_sent = (int) Metrics.globalRegistry
                .get("kafka.producer.record.send.total").tag("client-id", "producer").gauge().value();
        assertEquals(1, total_records_sent);

        producer.close();
    }

    @Test
    public void testConsumerMetrics() {

        HashMap<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, kafkaRule.getEmbeddedKafka().getBrokersAsString());
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(GROUP_ID_CONFIG, "Test");
        configs.put(CLIENT_ID_CONFIG, "consumer");
        configs.put(METRIC_REPORTER_CLASSES_CONFIG, MicrometerKafkaMetricsReporter.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        consumer.listTopics();

        int connection_count = (int) Metrics.globalRegistry
                .get("kafka.consumer.connection.count").tag("client-id", "consumer").gauge().value();
        assertEquals(1, connection_count);

        consumer.close();
    }
}