package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppianSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(AppianSinkConnector.class);

    public static final String APPIAN_ENDPOINT_CONFIG = "appian.endpoint.url";
    public static final String APPIAN_API_KEY_CONFIG = "appian.api.key";
    public static final String KAFKA_TOPIC_CONFIG = "topics"; // Standard Kafka Connect config for topics

    private Map<String, String> configProperties;

    @Override
    public String version() {
        // Replace with your connector's version
        return "1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting AppianSinkConnector");
        this.configProperties = props;
        // Validate required configurations
        if (props.get(APPIAN_ENDPOINT_CONFIG) == null || props.get(APPIAN_ENDPOINT_CONFIG).isEmpty()) {
            throw new ConfigException("Missing required configuration: " + APPIAN_ENDPOINT_CONFIG);
        }
        if (props.get(APPIAN_API_KEY_CONFIG) == null || props.get(APPIAN_API_KEY_CONFIG).isEmpty()) {
            throw new ConfigException("Missing required configuration: " + APPIAN_API_KEY_CONFIG);
        }
        if (props.get(KAFKA_TOPIC_CONFIG) == null || props.get(KAFKA_TOPIC_CONFIG).isEmpty()) {
            throw new ConfigException("Missing required configuration: " + KAFKA_TOPIC_CONFIG);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AppianSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Setting task configurations for {} tasks.", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        Map<String, String> taskProps = new HashMap<>(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(taskProps)); // Each task gets a copy of the connector config
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping AppianSinkConnector");
        // Nothing to do for now
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(APPIAN_ENDPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Appian API endpoint URL for creating records.")
                .define(APPIAN_API_KEY_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Appian API Key for authentication.")
                .define(KAFKA_TOPIC_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Comma-separated list of Kafka topics to consume from.");
    }
} 