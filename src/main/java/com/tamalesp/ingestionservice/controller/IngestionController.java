package com.tamalesp.ingestionservice.controller;

import com.tamalesp.ingestionservice.model.LogBatch;
import com.tamalesp.ingestionservice.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.security.Principal;

@RestController
@RequestMapping("/api/v1")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/logs")
    public Mono<ResponseEntity<String>> ingestLogs (@Valid @RequestBody Mono<LogBatch> request, Principal principal) {
        String tenantId = principal.getName();
        return request
                .flatMap(req -> ingestionService.processAndPublish(req.getLogs(), tenantId))
                .then(Mono.just(ResponseEntity.accepted().body("Logs successfully queued for ingestion for "+ tenantId)));
    }
}
