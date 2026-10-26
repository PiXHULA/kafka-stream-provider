package se.joakimonnhage.kafkastreamprovider.dto;

/**
 * Represents one key/value entry as read back out of the member state store.
 * The value fields mirror the Avro {@code Member} schema.
 */
public class StoredMember {

    public String key;
    public String id;
    public String username;
    public long createdTimestamp;

    public StoredMember() {
    }

    public StoredMember(String key, String id, String username, long createdTimestamp) {
        this.key = key;
        this.id = id;
        this.username = username;
        this.createdTimestamp = createdTimestamp;
    }
}

