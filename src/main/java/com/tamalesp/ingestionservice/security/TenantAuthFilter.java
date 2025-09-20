package com.tamalesp.ingestionservice.security;


import jakarta.validation.constraints.NotNull;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Configuration
public class TenantAuthFilter implements WebFilter {

    @Override
    public Mono<Void> filter(@NotNull ServerWebExchange exchange, @NotNull WebFilterChain chain) {

        //  get the Authorization header
        String apiKey = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if(apiKey != null && apiKey.startsWith("Bearer ")) {
            String tenantId = validateApiKey(apiKey.substring(7));

            if(tenantId != null) {
                Authentication authentication = new UsernamePasswordAuthenticationToken(tenantId, null, null);
                SecurityContext context = new SecurityContextImpl(authentication);

                return chain.filter(exchange).contextWrite(ReactiveSecurityContextHolder.withSecurityContext(Mono.just(context)));
            }
        }
        return chain.filter(exchange);
    }

    // This is placeholder logic. In a real-world scenario, you would validate
    // the API key against a database or a service and return the corresponding tenant ID.
    private String validateApiKey(String token) {
        if("super-security-token-acme-corp".equals(token)) {
            return "acme-corp";
        }

        if ("super-security-token-global-corp".equals(token)) {
            return "global-corp";
        }
        return null;
    }
}
