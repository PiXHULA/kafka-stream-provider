package se.joakimonnhage.kafkastreamprovider.kafka.avro;

import org.apache.kafka.common.serialization.Serdes;
import se.joakimonnhage.kafkastreamprovider.avro.Member;

/**
 * Reusable Serde for Avro-generated {@link Member} values.
 */
public final class MemberSerde extends Serdes.WrapperSerde<Member> {

    public MemberSerde() {
        super(new MemberSerializer(), new MemberDeserializer());
    }

    public static MemberSerde instance() {
        return new MemberSerde();
    }
}

