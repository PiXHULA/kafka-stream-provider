package se.joakimonnhage.kafkastreamprovider.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import se.joakimonnhage.kafkastreamprovider.avro.Member;
import se.joakimonnhage.kafkastreamprovider.avro.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * Registers local Avro schemas into Schema Registry subjects used by this app.
 */
@ApplicationScoped
class SchemaRegistryInitializer {

    private static final Logger LOGGER = Logger.getLogger(SchemaRegistryInitializer.class.getName());
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    void registerSchemas(String schemaRegistryUrl, String messageTopic, String memberTopic) {
        waitUntilReady(schemaRegistryUrl, 60, 1000);
        registerSchema(schemaRegistryUrl, messageTopic + "-value", Message.getClassSchema().toString());
        registerSchema(schemaRegistryUrl, memberTopic + "-value", Member.getClassSchema().toString());
    }

    private void waitUntilReady(String schemaRegistryUrl, int attempts, long sleepMillis) {
        String healthUrl = normalizeBaseUrl(schemaRegistryUrl) + "/subjects";
        for (int i = 0; i < attempts; i++) {
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) new URL(healthUrl).openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
                int status = connection.getResponseCode();
                if (status >= 200 && status < 300) {
                    return;
                }
            } catch (IOException ignored) {
                // Retry until timeout.
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Schema Registry to become ready", e);
            }
        }

        throw new IllegalStateException("Schema Registry did not become ready in time: " + schemaRegistryUrl);
    }

    private void registerSchema(String schemaRegistryUrl, String subject, String schemaJson) {
        String endpoint = normalizeBaseUrl(schemaRegistryUrl) + "/subjects/" + subject + "/versions";
        String payload = "{\"schema\":\"" + escapeJson(schemaJson) + "\"}";

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(endpoint).openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.setRequestProperty("Content-Type", CONTENT_TYPE);

            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(payload.getBytes(StandardCharsets.UTF_8));
            }

            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new IllegalStateException("Failed to register schema for subject '" + subject + "' (HTTP " + status + ")");
            }
            LOGGER.info("Schema registered for subject " + subject);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to register schema for subject '" + subject + "'", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String normalizeBaseUrl(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private String escapeJson(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
