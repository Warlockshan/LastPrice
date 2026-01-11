package com.SP.LastPriceJava.controller;

import com.SP.LastPriceJava.kafka.PriceBatchMessage;
import com.SP.LastPriceJava.kafka.PriceBatchProducer;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/producer/batch")
public class PriceBatchProducerController {

    private final PriceBatchProducer producer;

    public PriceBatchProducerController(PriceBatchProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/start")
    public String start() {
        String batchId = UUID.randomUUID().toString();
        producer.send(newMessage(batchId, PriceBatchMessage.Type.START));
        return batchId;
    }

    @PostMapping("/{batchId}/chunk")
    public void chunk(@PathVariable String batchId,
                      @RequestBody PriceBatchMessage msg) {
        msg.batchId = batchId;
        msg.type = PriceBatchMessage.Type.CHUNK;
        producer.send(msg);
    }

    @PostMapping("/{batchId}/complete")
    public void complete(@PathVariable String batchId) {
        producer.send(newMessage(batchId, PriceBatchMessage.Type.COMPLETE));
    }

    @PostMapping("/{batchId}/cancel")
    public void cancel(@PathVariable String batchId) {
        producer.send(newMessage(batchId, PriceBatchMessage.Type.CANCEL));
    }

    private PriceBatchMessage newMessage(String batchId, PriceBatchMessage.Type type) {
        PriceBatchMessage msg = new PriceBatchMessage();
        msg.batchId = batchId;
        msg.type = type;
        return msg;
    }

}
