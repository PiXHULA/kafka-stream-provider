package se.joakimonnhage.kafkastreamprovider.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberSerializer;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerde;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerializer;

import java.util.Properties;

/**
 * Centralizes building of the various {@link Properties} objects needed by the
 * Kafka Streams app, producer and admin client.
 */
final class KafkaPropertiesFactory {

    private KafkaPropertiesFactory() {
    }

    static Properties streamsProps(String bootstrapServers, String applicationId, String stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageSerde.class.getName());
        // Kafka Streams itself creates a per-application-id subdirectory under this path
        // (state.dir/application.id/...) - do NOT append applicationId here too, or you end up
        // with a doubled-up path like .../kafka-stream-provider-app/kafka-stream-provider-app.
        props.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Paths.get(stateDir).toAbsolutePath().normalize().toString());
        props.put(StreamsConfig.consumerPrefix(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        return props;
    }

    static Properties producerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        return props;
    }

    static Properties memberProducerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MemberSerializer.class.getName());
        return props;
    }

    static Properties adminProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}

