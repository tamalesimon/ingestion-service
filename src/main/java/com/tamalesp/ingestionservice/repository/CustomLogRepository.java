package com.tamalesp.ingestionservice.repository;

import com.tamalesp.ingestionservice.model.LogEntry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CustomLogRepository {
    Mono<Void> saveAllInTenantSchema(Flux<LogEntry> logs);
}
