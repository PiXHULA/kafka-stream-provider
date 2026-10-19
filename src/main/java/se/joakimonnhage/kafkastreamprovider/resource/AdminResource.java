package se.joakimonnhage.kafkastreamprovider.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import se.joakimonnhage.kafkastreamprovider.kafka.KafkaStreamsManager;

/**
 * Administrative operations for trying the stream out repeatedly.
 */
@Path("/admin")
public class AdminResource {

    @Inject
    KafkaStreamsManager kafkaStreamsManager;

    @POST
    @Path("/reset-offsets")
    public Response resetOffsets() {
        kafkaStreamsManager.resetOffsets();
        return Response.ok("{\"status\":\"offsets reset to 0, stream restarted\"}").build();
    }
}

