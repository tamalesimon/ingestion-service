package com.tamalesp.ingestionservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;


@Data
public class LogEntry {

    @NotBlank
    @JsonProperty("log_level")
    private String logLevel;

    @NotBlank
    private String message;

    @NotNull
    private String timestamp;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;
}
