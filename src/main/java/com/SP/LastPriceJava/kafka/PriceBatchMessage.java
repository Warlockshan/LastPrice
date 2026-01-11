package com.SP.LastPriceJava.kafka;

import com.SP.LastPriceJava.model.PriceEvent;

import java.util.List;

public class PriceBatchMessage {

    public enum Type {
        START,
        CHUNK,
        COMPLETE,
        CANCEL
    }

    public String batchId;
    public Type type;
    public List<PriceEvent> events;

}
