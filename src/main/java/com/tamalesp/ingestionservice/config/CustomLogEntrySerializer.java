package com.tamalesp.ingestionservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tamalesp.ingestionservice.model.LogEntry;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomLogEntrySerializer implements Serializer<LogEntry> {

    private final ObjectMapper objectMapper;

    public CustomLogEntrySerializer() {
        objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // add what to configure
    }

    @Override
    public byte[] serialize(String topic, LogEntry data) {
        if (data == null) {
            return null;
        }
        try{
            return objectMapper.writeValueAsBytes(data);
        }catch(Exception e){
            throw new SerializationException("Error serializing LogEntry to Json", e);
        }
    }

    @Override
    public void close() {}
}
