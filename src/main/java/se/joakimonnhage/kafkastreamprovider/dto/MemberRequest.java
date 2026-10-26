package se.joakimonnhage.kafkastreamprovider.dto;

/**
 * Request body used to submit a new member to the member topic.
 */
public class MemberRequest {

    public String username;

    public MemberRequest() {
    }

    public MemberRequest(String username) {
        this.username = username;
    }
}

