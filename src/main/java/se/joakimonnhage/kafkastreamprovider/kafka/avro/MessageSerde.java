package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import se.joakimonnhage.kafkastreamprovider.avro.Message;

import java.util.Map;

/**
 * Reusable Serde for Avro-generated {@link Message} values.
 */
public final class MessageSerde {

    private MessageSerde() {
    }

    public static SpecificAvroSerde<Message> instance(String schemaRegistryUrl) {
        SpecificAvroSerde<Message> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }
}

