package se.joakimonnhage.kafkastreamprovider.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Loads app configuration from META-INF/config.properties and applies optional
 * environment variable overrides.
 */
final class AppConfig {

    private static final String CONFIG_RESOURCE = "META-INF/config.properties";

    private static final Map<String, String[]> ENV_OVERRIDES = Map.of(
            "kafka.bootstrap.servers", new String[]{"KAFKA_BOOTSTRAP_SERVERS"},
            "kafka.message.topic", new String[]{"KAFKA_MESSAGE_TOPIC", "KAFKA_TOPIC"},
            "kafka.member.topic", new String[]{"KAFKA_MEMBER_TOPIC"},
            "kafka.application.id", new String[]{"KAFKA_APPLICATION_ID"},
            "kafka.state.dir", new String[]{"KAFKA_STATE_DIR"},
            "kafka.schema.registry.url", new String[]{"KAFKA_SCHEMA_REGISTRY_URL"}
    );

    private final String bootstrapServers;
    private final String messageTopic;
    private final String memberTopic;
    private final String applicationId;
    private final String stateDir;
    private final String schemaRegistryUrl;

    private AppConfig(String bootstrapServers,
                      String messageTopic,
                      String memberTopic,
                      String applicationId,
                      String stateDir,
                      String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.messageTopic = messageTopic;
        this.memberTopic = memberTopic;
        this.applicationId = applicationId;
        this.stateDir = stateDir;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    static AppConfig load() {
        Properties properties = new Properties();

        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing required classpath resource: " + CONFIG_RESOURCE);
            }
            properties.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read configuration from " + CONFIG_RESOURCE, e);
        }

        applyEnvOverrides(properties);

        return new AppConfig(
                require(properties, "kafka.bootstrap.servers"),
                require(properties, "kafka.message.topic"),
                require(properties, "kafka.member.topic"),
                require(properties, "kafka.application.id"),
                require(properties, "kafka.state.dir"),
                require(properties, "kafka.schema.registry.url")
        );
    }

    private static void applyEnvOverrides(Properties properties) {
        for (Map.Entry<String, String[]> entry : ENV_OVERRIDES.entrySet()) {
            for (String envVar : entry.getValue()) {
                String envValue = System.getenv(envVar);
                if (envValue != null && !envValue.isBlank()) {
                    properties.setProperty(entry.getKey(), envValue);
                    break;
                }
            }
        }
    }

    private static String require(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required config property: " + key);
        }
        return value;
    }

    String bootstrapServers() {
        return bootstrapServers;
    }

    String messageTopic() {
        return messageTopic;
    }

    String memberTopic() {
        return memberTopic;
    }

    String applicationId() {
        return applicationId;
    }

    String stateDir() {
        return stateDir;
    }

    String schemaRegistryUrl() {
        return schemaRegistryUrl;
    }
}

