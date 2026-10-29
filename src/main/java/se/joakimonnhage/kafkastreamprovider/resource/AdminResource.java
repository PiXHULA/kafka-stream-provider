package se.joakimonnhage.kafkastreamprovider.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.joakimonnhage.kafkastreamprovider.kafka.KafkaStreamsManager;

import java.util.List;

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
        return Response.ok("{\"status\":\"offsets reset to 0 for configured topics, stream restarted\"}").build();
    }

    @POST
    @Path("/create-topics")
    public Response createTopics() {
        kafkaStreamsManager.createConfiguredTopics();
        return Response.ok("{\"status\":\"topic creation attempted for configured topics\"}").build();
    }

    @GET
    @Path("/topics")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getAllTopics() {
        return kafkaStreamsManager.listTopics();
    }
}

