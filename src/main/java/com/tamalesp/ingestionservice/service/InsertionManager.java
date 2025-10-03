package com.tamalesp.ingestionservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tamalesp.ingestionservice.model.LogEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import io.r2dbc.spi.Parameters;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
public class InsertionManager {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;

    public InsertionManager(DatabaseClient databaseClient, ObjectMapper objectMapper) {
        this.databaseClient = databaseClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Dynamically inserts a LogEntry into a table based on the tenant ID.
     * It ensures the table exists before attempting the insert.
     * * @param tenantId The identifier used to name the table (e.g., "encot").
     *
     * @param logEntries The log data to insert.
     * @return Mono<Integer> indicating the number of rows inserted (should be 1).
     */
    public Mono<Void> insertLog(String tenantId, List<LogEntry> logEntries) {
        if (logEntries == null || logEntries.isEmpty()) {
            return Mono.empty();
        }

        String tableName = getTableName(tenantId);

        // 1. Ensure the table exists
        return ensureTableExists(tableName)
                .thenMany(Flux.fromIterable(logEntries)
                        // 2. Map each log entry to a single insert operation
                        .flatMap(logEntry -> executeInsert(tableName, logEntry)))
                .then(); // 3. Wait for all inserts to complete and return Mono<Void>
    }

    private String getTableName(String tenantId) {
        // Simple, predictable table naming convention (e.g., logs_encot)
        // IMPORTANT: Sanitize the tenantId to prevent SQL injection!
        String safeTenantId = tenantId.replaceAll("[^a-zA-Z0-9_]", "");
        return "logs_" + safeTenantId.toLowerCase();
    }

    private Mono<Void> ensureTableExists(String tableName) {
        // DDL statement to create the table if it does not exist
        String createTableSql = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                id UUID PRIMARY KEY,
                loglevel VARCHAR(50),
                message TEXT,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                metadata JSONB
            )
            """, tableName);

        return databaseClient.sql(createTableSql)
                .fetch()
                .rowsUpdated()
                .then(); // Return Mono<Void>
    }

    private Mono<Long> executeInsert(String tableName, LogEntry logEntry) {

        log.info("Inserting log entry into table {} {}", tableName, logEntry);
        // 1. Serialize Metadata
        String metadataJson;
        try {
            // Assume objectMapper is available
            metadataJson = objectMapper.writeValueAsString(logEntry.getMetadata());
        } catch (JsonProcessingException e) {
            return Mono.error(new IllegalStateException("Failed to serialize metadata for tenant " + tableName, e));
        }

        // 2. Build the INSERT statement
        String insertSql = String.format("""
        INSERT INTO %s (id, loglevel, message, timestamp, metadata)
        VALUES ($1, $2, $3, $4, $5::JSONB)
        """, tableName);

        // 3. Start the execution specification
        GenericExecuteSpec executeSpec = databaseClient.sql(insertSql);

        // --- THE CORRECTED FIX: Use Parameters.in(null, Class.class) for nulls ---

//        executeSpec = executeSpec
                // Parameter $1: log_level (String)
        // 1. For log_level (which is null in your example):
        executeSpec = executeSpec.bind("$1",
                logEntry.getId() != null
                        ? logEntry.getId()
                        : Parameters.in(String.class)
        );

                // Parameter $2: message (String)
        executeSpec = executeSpec.bind("$2", logEntry.getLoglevel() != null
                        ? logEntry.getLoglevel()
                        : Parameters.in(String.class));

                // Parameter $3: timestamp (Instant)
                // Note: java.time.Instant.class works, but you could also use OffsetDateTime.class or ZonedDateTime.class
        // 2. For timestamp (if it can be null):
        executeSpec = executeSpec.bind("$3",
                logEntry.getMessage() != null
                        ? logEntry.getMessage()
                        : Parameters.in(String.class)
        );

        // 2. For timestamp (if it can be null):
        executeSpec = executeSpec.bind("$4",
                logEntry.getTimestamp() != null
                        ? logEntry.getTimestamp()
                        : Parameters.in(Instant.class)
        );

        // 3. For metadata (assuming it was serialized to a nullable String/JSONB):
        executeSpec = executeSpec.bind("$5",
                metadataJson != null
                        ? metadataJson
                        : Parameters.in(String.class) // Use String.class since it's being bound as a String for JSONB
        );
        // ----------------------------------------------------------------------

        return executeSpec
                .fetch()
                .rowsUpdated(); // Returns Mono<Integer>
    }
}
