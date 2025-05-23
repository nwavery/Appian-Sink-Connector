# Kafka to Appian Sink Connector

This Kafka Connect sink connector consumes JSON messages from specified Kafka topics and sends them to an Appian instance using the Appian Suite API to create records.

## Overview

The connector performs the following actions:
- Subscribes to one or more Kafka topics.
- Expects JSON string messages from these topics.
- Authenticates with Appian using a provided API key.
- Makes HTTP POST requests to a configurable Appian API endpoint to create records.
- Includes a simple retry mechanism for failed API calls.

## Prerequisites

- Java 8 or higher
- Apache Maven 3.5.x or higher (for building)
- Access to a Kafka cluster (Confluent Cloud, or self-managed)
- An Appian instance with an API endpoint for record creation and a valid API key.

## Building the Connector

1.  **Clone the repository (if applicable) or ensure you have the source code.**
2.  **Build the connector using Maven:**
    Navigate to the project's root directory (where `pom.xml` is located) and run:
    ```bash
    mvn clean package -U
    ```
    This command will compile the code, run tests (if any), and package the connector.
    Two main artifacts will be created in the `target/` directory:
    1.  `kafka-to-appian-1.0-SNAPSHOT.jar`: The main connector JAR.
    2.  `kafka-to-appian-1.0-SNAPSHOT-custom-connector-connector.zip`: A ZIP archive ready for deployment. This archive contains the main connector JAR and all its necessary runtime dependencies in a `lib/` subdirectory, structured for platforms like Confluent Cloud.

## Packaging for Deployment (especially for Confluent Cloud)

The `maven-assembly-plugin` is configured in this project to automatically create the required ZIP archive during the `mvn package` phase. The generated ZIP file (e.g., `kafka-to-appian-1.0-SNAPSHOT-custom-connector-connector.zip`) is what you should use for uploading your custom connector to Confluent Cloud or deploying to other Kafka Connect environments that expect a plugin archive.

This ZIP file will have the following structure, including your connector JAR and its dependencies in the `lib/` folder:

```
kafka-to-appian-1.0-SNAPSHOT-custom-connector-connector.zip
├── lib/
│   ├── kafka-to-appian-1.0-SNAPSHOT.jar  (Your connector JAR)
│   ├── httpclient-4.5.13.jar
│   ├── httpcore-4.4.13.jar
│   ├── commons-logging-1.2.jar
│   ├── commons-codec-1.11.jar
│   ├── jackson-databind-2.12.3.jar
│   ├── jackson-annotations-2.12.3.jar
│   ├── jackson-core-2.12.3.jar
│   └── slf4j-simple-1.7.32.jar
└── ... (any other files specified in src/main/assembly/connector.xml, like a manifest.json if you add it)
```

If you wish to include a `manifest.json` file in the root of your ZIP (recommended by Confluent), create `manifest.json` in your project's root directory and uncomment the `<files>` section in `src/main/assembly/connector.xml`.

## Configuration Properties

When setting up an instance of this connector in the Confluent Cloud UI (or similar Kafka Connect management interfaces), you will typically provide the following configuration properties as key/value pairs. The "Key" is the property name listed below, and the "Value" is what you configure for your specific setup.

-   `name`
    -   **Description**: A unique name for this connector instance.
    -   **Example**: `appian-sink-prod-customers`
-   `connector.class`
    -   **Description**: The fully qualified name of the connector class.
    -   **Example**: `com.example.AppianSinkConnector`
-   `tasks.max`
    -   **Description**: The maximum number of tasks that should be created for this connector.
    -   **Example**: `1`
-   `topics`
    -   **Description**: A comma-separated list of Kafka topics to consume messages from.
    -   **Example**: `json_orders_topic,json_user_updates_topic`
-   `appian.endpoint.url`
    -   **Description**: The full HTTP(S) endpoint URL of the Appian API for creating records.
    -   **Example**: `https://your-appian-site.com/suite/webapi/your-record-creation-api`
-   `appian.api.key`
    -   **Description**: The API key for authenticating with the Appian API. (Mark as sensitive when configuring in Confluent Cloud).
    -   **Example**: `YOUR_SECRET_APPIAN_API_KEY`
-   `key.converter`
    -   **Description**: Converter for message keys.
    -   **Example**: `org.apache.kafka.connect.storage.StringConverter`
-   `key.converter.schemas.enable` (Optional)
    -   **Description**: Set to `false` if keys don't have schemas.
    -   **Example**: `false`
-   `value.converter`
    -   **Description**: Converter for message values. Since messages are JSON strings, StringConverter is appropriate.
    -   **Example**: `org.apache.kafka.connect.storage.StringConverter`
-   `value.converter.schemas.enable` (Optional)
    -   **Description**: Set to `false` as the connector expects plain JSON strings for values.
    -   **Example**: `false`

**Note on `appian.api.key`**: When deploying to Confluent Cloud, ensure you declare `appian.api.key` as a "sensitive property" during the connector plugin upload process. This allows Confluent Cloud to manage it securely.

## Deploying to Confluent Cloud

1.  **Package the Connector**: Create the ZIP archive as described in the "Packaging for Deployment" section.
2.  **Upload to Confluent Cloud**:
    -   Log in to your Confluent Cloud account.
    -   Navigate to your Kafka cluster and then to the "Connectors" section.
    -   Choose the option to "Add plugin" or "Add custom connector."
    -   Provide the plugin details:
        -   **Connector plugin name**: e.g., "Appian Sink Connector"
        -   **Custom plugin description**: A brief description.
        -   **Connector class**: `com.example.AppianSinkConnector`
        -   **Connector type**: `Sink`
    -   Upload your connector ZIP file.
    -   **Crucially, declare `appian.api.key` as a sensitive configuration property.**
    -   Submit the plugin.
3.  **Configure and Launch the Connector Instance**:
    -   Once the plugin is available, select it to create a new connector instance.
    -   Provide Kafka cluster credentials as required by your Confluent Cloud setup.
    -   Fill in all the configuration properties listed in the "Configuration Properties" section above, including your specific Appian endpoint URL, API key, and target Kafka topics.
    -   Specify necessary networking egress rules (e.g., for your Appian instance's hostname and API port).
    -   Set the number of tasks.
    -   Launch the connector.

For detailed, step-by-step instructions on using the Confluent Cloud UI or CLI to upload and manage custom connectors, please refer to the official [Confluent Cloud documentation on Custom Connectors](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/overview.html).

## Troubleshooting

-   Check the Kafka Connect worker logs for your connector tasks. In Confluent Cloud, these logs are accessible through the UI.
-   Ensure the Appian API endpoint is reachable from the environment where the Connect worker is running.
-   Verify that the Appian API key has the necessary permissions to create records.
-   Confirm that messages in the Kafka topic are valid JSON strings as expected by the connector. 