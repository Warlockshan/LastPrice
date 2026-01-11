package com.SP.LastPriceJava.kafka;

import com.SP.LastPriceJava.batch.BatchState;
import com.SP.LastPriceJava.batch.BatchStateManager;
import com.SP.LastPriceJava.batch.BatchStatus;
import com.SP.LastPriceJava.service.InMemoryPriceStore;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PriceBatchConsumer {

    private final BatchStateManager batchManager;
    private final InMemoryPriceStore store;

    public PriceBatchConsumer(BatchStateManager batchManager,
                              InMemoryPriceStore store) {
        this.batchManager = batchManager;
        this.store = store;
    }

    @KafkaListener(topics = "price-batch-topic")
    public void consume(PriceBatchMessage msg) {

        switch (msg.type) {
            case START -> batchManager.start(msg.batchId);

            case CHUNK -> {
                BatchState state = batchManager.get(msg.batchId);
                if (state != null && state.status == BatchStatus.STARTED) {
                    state.buffer.addAll(msg.events);
                }
            }

            case COMPLETE -> {
                BatchState state = batchManager.complete(msg.batchId);
                if (state != null && state.status == BatchStatus.COMPLETED) {
                    store.materialize(state.buffer);
                    batchManager.remove(msg.batchId);
                }
            }

            case CANCEL -> batchManager.cancel(msg.batchId);
        }
    }

}
