package se.joakimonnhage.kafkastreamprovider.kafka;

import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.avro.Message;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MessageSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MessageSerdeTest {

    @Test
    void configuredSpecificAvroSerdeRoundTripMessage() {
        Message original = Message.newBuilder()
                .setId("id-456")
                .setTimestamp(1735689600123L)
                .setMessage("serde wrapper")
                .build();

        byte[] bytes = null;
        Message restored = null;

        try (var serde = MessageSerde.instance("mock://message-serde-test")) {
            bytes = serde.serializer().serialize("messages", original);
            restored = serde.deserializer().deserialize("messages", bytes);
        }

        assertNotNull(bytes);
        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getTimestamp(), restored.getTimestamp());
        assertEquals(original.getMessage().toString(), restored.getMessage().toString());
    }
}

