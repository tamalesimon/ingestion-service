package com.tamalesp.ingestionservice.service;

import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class SchemaSwitchingService {

    private final R2dbcEntityTemplate r2dbcTemplate;

    public SchemaSwitchingService(R2dbcEntityTemplate r2dbcTemplate) {
        this.r2dbcTemplate = r2dbcTemplate;
    }

    public <T> Mono<T> switchSchema(Mono<T> dbOperation) {
        return Mono.deferContextual(ctx -> {
            String schemaName = ctx.get("schemaName");
            String switchSchemaSql = "SET search_path TO " + schemaName + ", public;";

            return r2dbcTemplate.getDatabaseClient().sql(switchSchemaSql).fetch().rowsUpdated()
                    .then(Mono.defer(() -> dbOperation));
        });
    }
}
