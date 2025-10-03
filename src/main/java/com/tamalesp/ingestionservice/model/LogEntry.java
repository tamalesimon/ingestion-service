package com.tamalesp.ingestionservice.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;


import java.time.Instant;
import java.util.UUID;


@AllArgsConstructor
@NoArgsConstructor
@Data
public class LogEntry {

    @Id
    private UUID id = UUID.randomUUID();
    private String loglevel;
    private String message;
    private Instant timestamp;
    private Object metadata;
}
