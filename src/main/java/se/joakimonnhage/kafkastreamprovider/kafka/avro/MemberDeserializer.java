package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import se.joakimonnhage.kafkastreamprovider.avro.Member;

import java.io.IOException;

/**
 * Kafka deserializer for Avro-generated {@link Member} records.
 */
public final class MemberDeserializer implements Deserializer<Member> {

    @Override
    public Member deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return new SpecificDatumReader<>(Member.class).read(null, decoder);
        } catch (IOException e) {
            throw new org.apache.kafka.common.errors.SerializationException(
                    "Failed to deserialize Member for topic '" + topic + "'", e);
        }
    }
}

