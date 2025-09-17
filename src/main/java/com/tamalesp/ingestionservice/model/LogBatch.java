package com.tamalesp.ingestionservice.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import lombok.Data;

import java.util.List;

@Data
public class LogBatch {

    @NotEmpty
    @Valid
    private List<LogEntry> logs;
}
