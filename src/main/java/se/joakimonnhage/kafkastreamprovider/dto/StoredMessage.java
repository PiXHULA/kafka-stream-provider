package se.joakimonnhage.kafkastreamprovider.dto;

/**
 * Represents one key/value entry as read back out of the Kafka Streams state store.
 */
public class StoredMessage {

    public String key;
    public String value;

    public StoredMessage() {
    }

    public StoredMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }
}

