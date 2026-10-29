package se.joakimonnhage.kafkastreamprovider.kafka;

import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.avro.Member;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MemberSerdeTest {

    @Test
    void configuredSpecificAvroSerdeRoundTripMember() {
        Member original = Member.newBuilder()
                .setId("member-456")
                .setUsername("bob")
                .setCreatedTimestamp(1735689600123L)
                .build();

        byte[] bytes;
        Member restored;

        try (var serde = MemberSerde.instance("mock://member-serde-test")) {
            bytes = serde.serializer().serialize("new-member", original);
            restored = serde.deserializer().deserialize("new-member", bytes);
        }

        assertNotNull(bytes);
        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getUsername().toString(), restored.getUsername().toString());
        assertEquals(original.getCreatedTimestamp(), restored.getCreatedTimestamp());
    }
}

