package com.SP.LastPriceJava.model;

import java.time.Instant;
import java.util.Map;

public class PriceEvent {

    public String id;
    public Instant asOf;
    public Map<String, Object> payload;

}
