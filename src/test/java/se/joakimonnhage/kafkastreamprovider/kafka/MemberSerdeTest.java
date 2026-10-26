package se.joakimonnhage.kafkastreamprovider.kafka;

import org.junit.jupiter.api.Test;
import se.joakimonnhage.kafkastreamprovider.avro.Member;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberDeserializer;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberSerde;
import se.joakimonnhage.kafkastreamprovider.kafka.avro.MemberSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MemberSerdeTest {

    @Test
    void serializerAndDeserializerRoundTripMember() {
        Member originalMember = Member.newBuilder()
                .setId("member-123")
                .setUsername("alice")
                .setCreatedTimestamp(1735689600000L)
                .build();

        byte[] serializedMember;
        Member deserializedMember;

        try (MemberSerializer serializer = new MemberSerializer(); MemberDeserializer deserializer = new MemberDeserializer()) {
            serializedMember = serializer.serialize("new-member", originalMember);
            deserializedMember = deserializer.deserialize("new-member", serializedMember);
        }

        assertNotNull(serializedMember);
        assertNotNull(deserializedMember);
        assertEquals(originalMember.getId().toString(), deserializedMember.getId().toString());
        assertEquals(originalMember.getUsername().toString(), deserializedMember.getUsername().toString());
        assertEquals(originalMember.getCreatedTimestamp(), deserializedMember.getCreatedTimestamp());
    }

    @Test
    void wrapperSerdeRoundTripMember() {
        Member original = Member.newBuilder()
                .setId("member-456")
                .setUsername("bob")
                .setCreatedTimestamp(1735689600123L)
                .build();

        byte[] bytes;
        Member restored;

        try (MemberSerde serde = MemberSerde.instance()) {
            bytes = serde.serializer().serialize("new-member", original);
            restored = serde.deserializer().deserialize("new-member", bytes);
        }

        assertNotNull(restored);
        assertEquals(original.getId().toString(), restored.getId().toString());
        assertEquals(original.getUsername().toString(), restored.getUsername().toString());
        assertEquals(original.getCreatedTimestamp(), restored.getCreatedTimestamp());
    }
}

