package com.tamalesp.ingestionservice.impl;

import com.tamalesp.ingestionservice.model.LogEntry;
import com.tamalesp.ingestionservice.repository.CustomLogRepository;
import com.tamalesp.ingestionservice.service.SchemaSwitchingService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class LogRepositoryImpl implements CustomLogRepository {

    private final R2dbcEntityTemplate r2dbcEntityTemplate;

    private final SchemaSwitchingService schemaSwitchingService;

    @Override
    public Mono<Void> saveAllInTenantSchema(Flux<LogEntry> logs) {

        Mono<Void> saveOperation = logs
                .flatMap(log -> r2dbcEntityTemplate.insert(LogEntry.class).using(log))
                .then();

//        Mono<Void> saveOperation = r2dbcEntityTemplate.insert(LogEntry.class)
//                .all(logs)
//                .then();

        return schemaSwitchingService.switchSchema(saveOperation);
    }
}
