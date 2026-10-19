package se.joakimonnhage.kafkastreamprovider.dto;

/**
 * Request body used to submit a new piece of data to the input topic.
 */
public class MessageRequest {

    public String message;

    public MessageRequest() {
    }

    public MessageRequest(String message) {
        this.message = message;
    }
}

