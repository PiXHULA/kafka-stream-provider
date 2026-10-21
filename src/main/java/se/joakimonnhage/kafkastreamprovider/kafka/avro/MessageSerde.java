package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import org.apache.kafka.common.serialization.Serdes;
import se.joakimonnhage.kafkastreamprovider.avro.Message;

/**
 * Reusable Serde for Avro-generated {@link Message} values.
 */
public final class MessageSerde extends Serdes.WrapperSerde<Message> {

    public MessageSerde() {
        super(new MessageSerializer(), new MessageDeserializer());
    }

    public static MessageSerde instance() {
        return new MessageSerde();
    }
}

