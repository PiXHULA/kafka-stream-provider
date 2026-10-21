package se.joakimonnhage.kafkastreamprovider.dto;

/**
 * Represents one key/value entry as read back out of the Kafka Streams state store.
 * The value fields mirror the Avro {@code Message} schema (id, timestamp, message).
 */
public class StoredMessage {

    public String key;
    public String id;
    public long timestamp;
    public String message;

    public StoredMessage() {
    }

    public StoredMessage(String key, String id, long timestamp, String message) {
        this.key = key;
        this.id = id;
        this.timestamp = timestamp;
        this.message = message;
    }
}

