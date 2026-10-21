package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import se.joakimonnhage.kafkastreamprovider.avro.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Kafka serializer for Avro-generated {@link Message} records.
 */
public final class MessageSerializer implements Serializer<Message> {

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<>(Message.class).write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new org.apache.kafka.common.errors.SerializationException(
                    "Failed to serialize Message for topic '" + topic + "'", e);
        }
    }
}

