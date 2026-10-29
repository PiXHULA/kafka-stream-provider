package se.joakimonnhage.kafkastreamprovider.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPropertiesFactoryTest {

    @Test
    void streamsPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.streamsProps(
                "localhost:9092", "my-app-id", "some/relative/state-dir", "http://localhost:8081");

        assertEquals("my-app-id", props.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("localhost:9092", props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
                props.get(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(SpecificAvroSerde.class.getName(),
                props.get(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
        assertEquals("http://localhost:8081", props.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        assertEquals("earliest",
                props.get(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));
    }

    @Test
    void streamsPropsNormalizesStateDirToAbsolutePath() {
        Properties props = KafkaPropertiesFactory.streamsProps(
                "localhost:9092", "my-app-id", "some/relative/state-dir", "http://localhost:8081");

        String expected = Paths.get("some/relative/state-dir").toAbsolutePath().normalize().toString();
        assertEquals(expected, props.get(StreamsConfig.STATE_DIR_CONFIG));
    }

    @Test
    void messageProducerPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.messageProducerProps("localhost:9092", "http://localhost:8081");

        assertEquals("localhost:9092", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroSerializer.class.getName(), props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("http://localhost:8081", props.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void memberMessageProducerPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.memberProducerProps("localhost:9092", "http://localhost:8081");

        assertEquals("localhost:9092", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroSerializer.class.getName(), props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("http://localhost:8081", props.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void adminPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.adminProps("localhost:9092");

        assertEquals("localhost:9092", props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(1, props.size());
    }
}

