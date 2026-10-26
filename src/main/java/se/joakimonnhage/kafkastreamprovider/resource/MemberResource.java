package se.joakimonnhage.kafkastreamprovider.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.joakimonnhage.kafkastreamprovider.dto.MemberRequest;
import se.joakimonnhage.kafkastreamprovider.dto.StoredMember;
import se.joakimonnhage.kafkastreamprovider.kafka.KafkaStreamsManager;

import java.util.List;

/**
 * Lets you submit new members onto Kafka and read back all members consumed so far.
 */
@Path("/members")
public class MemberResource {

    @Inject
    KafkaStreamsManager kafkaStreamsManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submit(MemberRequest request) {
        if (request == null || request.username == null || request.username.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("{\"error\":\"username must not be blank\"}")
                    .build();
        }
        try {
            kafkaStreamsManager.produceMember(request.username);
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("{\"error\":\"failed to produce member: " + e.getMessage() + "\"}")
                    .build();
        }
        return Response.accepted().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<StoredMember> getAll() {
        return kafkaStreamsManager.readAllMembers();
    }
}

