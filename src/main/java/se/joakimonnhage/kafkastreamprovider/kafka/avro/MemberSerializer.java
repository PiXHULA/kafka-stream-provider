package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import se.joakimonnhage.kafkastreamprovider.avro.Member;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Kafka serializer for Avro-generated {@link Member} records.
 */
public final class MemberSerializer implements Serializer<Member> {

    @Override
    public byte[] serialize(String topic, Member data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<>(Member.class).write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new org.apache.kafka.common.errors.SerializationException(
                    "Failed to serialize Member for topic '" + topic + "'", e);
        }
    }
}

