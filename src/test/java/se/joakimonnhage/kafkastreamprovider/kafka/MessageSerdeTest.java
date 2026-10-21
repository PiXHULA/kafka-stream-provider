package se.joakimonnhage.kafkastreamprovider.kafka;

import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.avro.Message;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageDeserializer;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerde;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class MessageSerdeTest {

    @Test
    void serializerAndDeserializerRoundTripMessage() {
        Message original = Message.newBuilder()
                .setId("id-123")
                .setTimestamp(1735689600000L)
                .setMessage("hello avro")
                .build();

        MessageSerializer serializer = new MessageSerializer();
        MessageDeserializer deserializer = new MessageDeserializer();

        byte[] bytes = serializer.serialize("messages", original);
        Message restored = deserializer.deserialize("messages", bytes);

        assertNotNull(bytes);
        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getTimestamp(), restored.getTimestamp());
        assertEquals(original.getMessage().toString(), restored.getMessage().toString());
    }

    @Test
    void serializerReturnsNullForNullMessage() {
        MessageSerializer serializer = new MessageSerializer();
        assertNull(serializer.serialize("messages", null));
    }

    @Test
    void deserializerReturnsNullForNullPayload() {
        MessageDeserializer deserializer = new MessageDeserializer();
        assertNull(deserializer.deserialize("messages", null));
    }

    @Test
    void wrapperSerdeRoundTripMessage() {
        Message original = Message.newBuilder()
                .setId("id-456")
                .setTimestamp(1735689600123L)
                .setMessage("serde wrapper")
                .build();

        MessageSerde serde = MessageSerde.instance();
        byte[] bytes = serde.serializer().serialize("messages", original);
        Message restored = serde.deserializer().deserialize("messages", bytes);

        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getTimestamp(), restored.getTimestamp());
        assertEquals(original.getMessage().toString(), restored.getMessage().toString());
    }
}

