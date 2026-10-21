package se.joakimonnhage.kafkastreamprovider.kafka;

import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.avro.Message;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageDeserializer;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerde;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MessageSerdeTest {

    @Test
    void serializerAndDeserializerRoundTripMessage() {
        Message originalMessage = Message.newBuilder()
                .setId("id-123")
                .setTimestamp(1735689600000L)
                .setMessage("hello avro")
                .build();

        byte[] serializedMessage;
        Message deserializedMessage;

        try (MessageSerializer serializer = new MessageSerializer(); MessageDeserializer deserializer = new MessageDeserializer()) {
            serializedMessage = serializer.serialize("messages", originalMessage);
            deserializedMessage = deserializer.deserialize("messages", serializedMessage);
        }

        assertNotNull(serializedMessage);
        assertNotNull(deserializedMessage);
        assertEquals(originalMessage.getId().toString(), deserializedMessage.getId().toString());
        assertEquals(originalMessage.getTimestamp(), deserializedMessage.getTimestamp());
        assertEquals(originalMessage.getMessage().toString(), deserializedMessage.getMessage().toString());
    }

    @Test
    void wrapperSerdeRoundTripMessage() {
        Message original = Message.newBuilder()
                .setId("id-456")
                .setTimestamp(1735689600123L)
                .setMessage("serde wrapper")
                .build();

        byte[] bytes = null;
        Message restored = null;

        try (MessageSerde serde = MessageSerde.instance()) {
            bytes = serde.serializer().serialize("messages", original);
            restored = serde.deserializer().deserialize("messages", bytes);
        }

        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getTimestamp(), restored.getTimestamp());
        assertEquals(original.getMessage().toString(), restored.getMessage().toString());
    }
}

