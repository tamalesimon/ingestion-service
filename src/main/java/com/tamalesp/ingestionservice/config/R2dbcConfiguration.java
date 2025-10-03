package com.tamalesp.ingestionservice.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper; // Ensure this is autowired/available
import io.r2dbc.postgresql.codec.Json;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.PostgresDialect;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class R2dbcConfiguration {

    // Inject the application's shared ObjectMapper
    private final ObjectMapper objectMapper;

    public R2dbcConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    public R2dbcCustomConversions r2dbcCustomConversions() {
        List<Converter<?, ?>> converters = List.of(
                new MapToJsonConverter(objectMapper),
                new JsonToMapConverter(objectMapper)
        );

        // CRITICAL FIX: Explicitly register converters for the Postgres Dialect
        return R2dbcCustomConversions.of(PostgresDialect.INSTANCE, converters);
    }


    // Converts Map<String, Object> (Java) to Json (R2DBC for JSONB)
    @WritingConverter
    static class MapToJsonConverter implements Converter<Map<String, Object>, Json> {
        private final ObjectMapper objectMapper; // Use the injected instance

        public MapToJsonConverter(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public Json convert(@NonNull Map<String, Object> source) {
            // log.info("MapToJsonConverter: {} ", source); // Keep this for debugging if needed
            try {
                String json = objectMapper.writeValueAsString(source); // Use shared instance
                return Json.of(json);
            } catch (Exception e) {
                throw new RuntimeException("Error converting Map to JSON", e);
            }
        }
    }


    @ReadingConverter
    static class JsonToMapConverter implements Converter<Json, Map<String, Object>> {
        private final ObjectMapper objectMapper; // Use the injected instance

        public JsonToMapConverter(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public Map<String, Object> convert(@NonNull Json source) {
            try {
                // Use shared instance
                return objectMapper.readValue(source.asString(), new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) {
                throw new RuntimeException("Error converting JSON to Map", e);
            }
        }
    }

}