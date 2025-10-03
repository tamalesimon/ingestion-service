package com.tamalesp.ingestionservice.component;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class TenantContextFilter implements WebFilter {

    @Override
    public @NonNull Mono<Void> filter(@NonNull ServerWebExchange exchange, @NonNull WebFilterChain chain) {
        return ReactiveSecurityContextHolder.getContext()
                .map(securityContext -> {
                    String clientId = securityContext.getAuthentication().getName();
                    return "tenant_" + clientId.toLowerCase().replace('-', '_');
                })
                .defaultIfEmpty("default_schema")
                .flatMap(schemaName ->
                        chain.filter(exchange)
                                .contextWrite(ctx -> ctx.put("schemaName", schemaName))
                );
    }
}
