package se.joakimonnhage.kafkastreamprovider.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerde;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerializer;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberSerializer;

import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPropertiesFactoryTest {

    @Test
    void streamsPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.streamsProps(
                "localhost:9092", "my-app-id", "some/relative/state-dir");

        assertEquals("my-app-id", props.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("localhost:9092", props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde",
                props.get(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(MessageSerde.class.getName(),
                props.get(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
        assertEquals("earliest",
                props.get(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));
    }

    @Test
    void streamsPropsNormalizesStateDirToAbsolutePath() {
        Properties props = KafkaPropertiesFactory.streamsProps(
                "localhost:9092", "my-app-id", "some/relative/state-dir");

        String expected = Paths.get("some/relative/state-dir").toAbsolutePath().normalize().toString();
        assertEquals(expected, props.get(StreamsConfig.STATE_DIR_CONFIG));
    }

    @Test
    void producerPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.producerProps("localhost:9092");

        assertEquals("localhost:9092", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(MessageSerializer.class.getName(), props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void memberProducerPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.memberProducerProps("localhost:9092");

        assertEquals("localhost:9092", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(MemberSerializer.class.getName(), props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void adminPropsContainsExpectedConfig() {
        Properties props = KafkaPropertiesFactory.adminProps("localhost:9092");

        assertEquals("localhost:9092", props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(1, props.size());
    }
}

