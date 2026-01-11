package com.SP.LastPriceJava.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PriceBatchProducer {

    private final KafkaTemplate<String, PriceBatchMessage> kafkaTemplate;

    public PriceBatchProducer(KafkaTemplate<String, PriceBatchMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(PriceBatchMessage message) {
        kafkaTemplate.send("price-batch-topic", message.batchId, message);
    }

}
