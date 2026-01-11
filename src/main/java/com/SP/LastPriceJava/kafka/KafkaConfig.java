package com.SP.LastPriceJava.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    public JsonSerializer<?> kafkaJsonSerializer(ObjectMapper kafkaObjectMapper) {
        return new JsonSerializer<>(kafkaObjectMapper);
    }

    @Bean
    public JsonDeserializer<?> kafkaJsonDeserializer(ObjectMapper kafkaObjectMapper) {
        JsonDeserializer<?> deserializer = new JsonDeserializer<>(kafkaObjectMapper);
        deserializer.addTrustedPackages("com.SP.LastPriceJava");
        return deserializer;
    }
}
