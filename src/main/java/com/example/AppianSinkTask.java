package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AppianSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(AppianSinkTask.class);

    private String appianEndpointUrl;
    private String appianApiKey;
    private CloseableHttpClient httpClient;
    private static final int MAX_RETRIES = 3;
    private AvroMapper avroMapper; // For Avro to JSON conversion

    // Batching specific properties
    private List<SinkRecord> batch;
    private int appianBatchSize;
    private long appianBatchMaxWaitMs;
    private Instant batchStartTime;

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
        this.avroMapper = new AvroMapper(); // Initialize AvroMapper

        // Initialize batching properties
        this.batch = new ArrayList<>();
        this.appianBatchSize = Integer.parseInt(props.getOrDefault(AppianSinkConnector.APPIAN_BATCH_SIZE_CONFIG, "100"));
        this.appianBatchMaxWaitMs = Long.parseLong(props.getOrDefault(AppianSinkConnector.APPIAN_BATCH_MAX_WAIT_MS_CONFIG, "5000"));
        this.batchStartTime = Instant.now();

        logger.info("AppianSinkTask configured with batch size: {} and max wait ms: {}", appianBatchSize, appianBatchMaxWaitMs);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        logger.debug("Received {} records.", records.size());
        for (SinkRecord record : records) {
            if (record.value() == null) {
                logger.warn("Received null record value, skipping. Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            batch.add(record);
        }

        // Check if batch should be processed
        if (!batch.isEmpty() && (batch.size() >= appianBatchSize ||
            Duration.between(batchStartTime, Instant.now()).toMillis() >= appianBatchMaxWaitMs)) {
            logger.info("Processing batch due to size ({}) or time ({} ms elapsed).", batch.size(), Duration.between(batchStartTime, Instant.now()).toMillis());
            processBatch();
        }
    }

    private void processBatch() {
        if (batch.isEmpty()) {
            return;
        }
        logger.info("Sending batch of {} records to Appian.", batch.size());
        List<SinkRecord> currentBatch = new ArrayList<>(batch); // Process a copy
        batch.clear(); // Clear original batch before processing to accept new records
        batchStartTime = Instant.now(); // Reset timer

        for (SinkRecord record : currentBatch) {
            String jsonData = null;
            try {
                Object value = record.value();
                if (value instanceof GenericRecord) {
                    GenericRecord avroRecord = (GenericRecord) value;
                    jsonData = avroMapper.writeValueAsString(avroRecord);
                    logger.debug("Converted Avro record to JSON for (Topic: {}, Partition: {}, Offset: {}): {}",
                                 record.topic(), record.kafkaPartition(), record.kafkaOffset(), jsonData);
                } else if (value instanceof String) {
                    jsonData = (String) value;
                    logger.debug("Using raw string as JSON for (Topic: {}, Partition: {}, Offset: {}): {}",
                                 record.topic(), record.kafkaPartition(), record.kafkaOffset(), jsonData);
                } else if (value == null) {
                    // This case should ideally be caught by the check in put(), but defensive check here.
                    logger.warn("Null record value encountered in processBatch for (Topic: {}, Partition: {}, Offset: {}), skipping.",
                                record.topic(), record.kafkaPartition(), record.kafkaOffset());
                    continue; // Skip this record
                } else {
                    logger.warn("Unexpected record value type {} encountered in processBatch for (Topic: {}, Partition: {}, Offset: {}), skipping.",
                                value.getClass().getName(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
                    continue; // Skip this record
                }

                if (jsonData != null) {
                    sendToAppianWithRetries(jsonData, record);
                }
            } catch (JsonProcessingException e) {
                logger.error("Failed to convert record to JSON for (Topic: {}, Partition: {}, Offset: {}). Error: {}",
                             record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage(), e);
                // Optional: throw new ConnectException("Failed to serialize record to JSON", e); to let Connect handle it (e.g., DLQ)
            } catch (Exception e) { // Catch any other unexpected errors during individual record processing
                logger.error("Unexpected error processing record (Topic: {}, Partition: {}, Offset: {}). Error: {}",
                             record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage(), e);
                // Optional: throw new ConnectException or handle error specific to this record
            }
        }
        logger.info("Finished processing batch of {} records.", currentBatch.size());
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
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("Flush called. Processing any remaining records in the batch (size: {}).", batch.size());
        processBatch();
        // The 'offsets' parameter is provided by the Connect framework. If your task manages offsets itself,
        // you would use this map to tell the framework which offsets have been successfully committed.
        // In this implementation, we are relying on the framework's default offset management, which typically commits
        // offsets for records that have been passed to put() and for which put() has returned successfully.
        // If processBatch() throws an exception for a record, the framework might retry or handle it based on its error settings.
        // For more complex scenarios, like wanting to commit offsets only *after* Appian confirms receipt of a batch,
        // you would need to store the offsets from the SinkRecords in the batch and use them here.
        // For now, this basic flush primarily ensures pending records are sent.
    }

    @Override
    public void stop() {
        logger.info("Stopping AppianSinkTask. Processing final batch (size: {}).", batch.size());
        processBatch(); // Ensure any remaining data is sent

        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                logger.warn("Error closing HttpClient", e);
            }
        }
    }
} 