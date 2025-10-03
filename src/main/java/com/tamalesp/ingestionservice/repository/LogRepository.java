package com.tamalesp.ingestionservice.repository;

import com.tamalesp.ingestionservice.model.LogEntry;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.UUID;

public interface LogRepository  extends ReactiveCrudRepository<LogEntry, UUID>, CustomLogRepository {
}
