package com.SP.LastPriceJava.batch;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class BatchStateManager {

    private final ConcurrentHashMap<String, BatchState> batches = new ConcurrentHashMap<>();

    public BatchState start(String batchId) {
        return batches.computeIfAbsent(batchId, id -> new BatchState());
    }

    public BatchState get(String batchId) {
        return batches.get(batchId);
    }

    public void cancel(String batchId) {
        BatchState state = batches.get(batchId);
        if (state != null) {
            state.status = BatchStatus.CANCELLED;
            state.buffer.clear();
        }
    }

    public BatchState complete(String batchId) {
        BatchState state = batches.get(batchId);
        if (state != null) {
            state.status = BatchStatus.COMPLETED;
        }
        return state;
    }

    public void remove(String batchId) {
        batches.remove(batchId);
    }

}
