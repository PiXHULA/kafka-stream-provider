package se.joakimonnhage.kafkastreamprovider.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.joakimonnhage.kafkastreamprovider.dto.MessageRequest;
import se.joakimonnhage.kafkastreamprovider.dto.StoredMessage;
import se.joakimonnhage.kafkastreamprovider.kafka.KafkaStreamsManager;

import java.util.List;

/**
 * Lets you submit new data onto the Kafka topic, and read back everything the
 * Kafka Streams app has consumed from it so far.
 */
@Path("/messages")
public class DataResource {

    @Inject
    KafkaStreamsManager kafkaStreamsManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submit(MessageRequest request) {
        if (request == null || request.message == null || request.message.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\":\"message must not be blank\"}")
                    .build();
        }
        try {
            kafkaStreamsManager.produce(request.message);
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("{\"error\":\"failed to produce message: " + e.getMessage() + "\"}")
                    .build();
        }
        return Response.accepted().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StoredMessage> getAll() {
        return kafkaStreamsManager.readAll();
    }
}

