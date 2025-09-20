package com.tamalesp.ingestionservice.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    private final TenantAuthFilter tenantAuthFilter;

    public  SecurityConfig(TenantAuthFilter tenantAuthFilter) {
        this.tenantAuthFilter = tenantAuthFilter;
    }

    @Bean
    public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
        return http
                // Disable CSRF since we are using stateless authentication
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                // add a custom authentication filter before the default authentication filter
                .addFilterAt(tenantAuthFilter, SecurityWebFiltersOrder.AUTHENTICATION)
                .authorizeExchange(exchanges -> exchanges

//                        // All requests to the logs endpoint must be authenticated
                        .pathMatchers("/api/v1/logs").authenticated()

                        // All other requests are permitted without authentication
                        .anyExchange().permitAll()
                )
                // using stateless session management
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .build();
    }

}
