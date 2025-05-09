package com.example;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class AppianSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AppianSinkTask.class);

    private String appianEndpointUrl;
    private String appianApiKey;
    private CloseableHttpClient httpClient;
    private static final int MAX_RETRIES = 3;

    @Override
    public String version() {
        return new AppianSinkConnector().version(); // Or define separately
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting AppianSinkTask");
        appianEndpointUrl = props.get(AppianSinkConnector.APPIAN_ENDPOINT_CONFIG);
        appianApiKey = props.get(AppianSinkConnector.APPIAN_API_KEY_CONFIG);

        if (appianEndpointUrl == null || appianEndpointUrl.isEmpty()) {
            throw new RuntimeException("Appian endpoint URL not configured.");
        }
        if (appianApiKey == null || appianApiKey.isEmpty()) {
            throw new RuntimeException("Appian API key not configured.");
        }
        this.httpClient = HttpClients.createDefault();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        logger.info("Received {} records to send to Appian.", records.size());
        for (SinkRecord record : records) {
            if (record.value() == null) {
                logger.warn("Received null record value, skipping. Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            // Assuming the record value is a JSON string, as per the original requirement
            String jsonData = record.value().toString();
            sendToAppianWithRetries(jsonData, record);
        }
    }

    private void sendToAppianWithRetries(String jsonData, SinkRecord record) {
        int attempt = 0;
        boolean success = false;
        while (attempt < MAX_RETRIES && !success) {
            attempt++;
            try {
                HttpPost postRequest = new HttpPost(appianEndpointUrl);
                postRequest.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + appianApiKey);
                postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                postRequest.setHeader("Appian-API-Key", appianApiKey);

                StringEntity entity = new StringEntity(jsonData);
                postRequest.setEntity(entity);

                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(5000) // 5 seconds
                        .setSocketTimeout(5000)  // 5 seconds
                        .build();
                postRequest.setConfig(requestConfig);

                logger.info("Attempt {} to send data to Appian for record (Topic: {}, Partition: {}, Offset: {}): {}",
                        attempt, record.topic(), record.kafkaPartition(), record.kafkaOffset(), jsonData);

                HttpResponse response = httpClient.execute(postRequest);
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity()); // Consume entity to close connection

                if (statusCode >= 200 && statusCode < 300) {
                    logger.info("Successfully sent data to Appian for record (Topic: {}, Partition: {}, Offset: {}). Status: {}. Response: {}",
                            record.topic(), record.kafkaPartition(), record.kafkaOffset(), statusCode, responseBody);
                    success = true;
                } else {
                    logger.error("Failed to send data to Appian for record (Topic: {}, Partition: {}, Offset: {}). Status: {}. Response: {}",
                            record.topic(), record.kafkaPartition(), record.kafkaOffset(), statusCode, responseBody);
                    if (attempt == MAX_RETRIES) {
                        logger.error("Max retries reached for record (Topic: {}, Partition: {}, Offset: {}). Message: {}",
                                record.topic(), record.kafkaPartition(), record.kafkaOffset(), jsonData);
                        // Optionally, you can throw an exception here to signal failure to Connect framework
                        // throw new ConnectException("Failed to send record to Appian after " + MAX_RETRIES + " attempts.");
                    } else {
                        logger.info("Retrying ({}/{}) for record (Topic: {}, Partition: {}, Offset: {})...",
                                attempt, MAX_RETRIES, record.topic(), record.kafkaPartition(), record.kafkaOffset());
                        Thread.sleep(1000 * attempt); // Simple exponential backoff
                    }
                }
            } catch (IOException e) {
                logger.error("IOException during Appian API call (attempt {}) for record (Topic: {}, Partition: {}, Offset: {}): {}",
                        attempt, record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage(), e);
                if (attempt == MAX_RETRIES) {
                     logger.error("Max retries reached due to IOException for record (Topic: {}, Partition: {}, Offset: {}). Message: {}",
                                record.topic(), record.kafkaPartition(), record.kafkaOffset(), jsonData);
                } else {
                    logger.info("Retrying ({}/{}) for record (Topic: {}, Partition: {}, Offset: {})...",
                                attempt, MAX_RETRIES, record.topic(), record.kafkaPartition(), record.kafkaOffset());
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Retry sleep interrupted for record (Topic: {}, Partition: {}, Offset: {}).",
                                record.topic(), record.kafkaPartition(), record.kafkaOffset(), ie);
                        // Propagate interrupt or handle as a permanent failure for the record
                        // For simplicity, we break here, but a ConnectException might be more appropriate.
                        break; 
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread interrupted during retry sleep for record (Topic: {}, Partition: {}, Offset: {}).",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
                // Propagate interrupt or handle as a permanent failure for the record
                break; 
            }
        }
        if (!success) {
            // If not successful after all retries, you might want to throw a ConnectException
            // to let the Connect framework handle the failure (e.g., send to dead letter queue).
            // For now, just logging an error.
            logger.error("Failed to send record to Appian after all retries: (Topic: {}, Partition: {}, Offset: {})",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping AppianSinkTask");
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                logger.warn("Error closing HttpClient", e);
            }
        }
    }
} 