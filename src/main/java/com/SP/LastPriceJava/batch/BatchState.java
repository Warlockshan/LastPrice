package com.SP.LastPriceJava.batch;

import com.SP.LastPriceJava.model.PriceEvent;

import java.util.ArrayList;
import java.util.List;

public class BatchState {
    public BatchStatus status = BatchStatus.STARTED;
    public List<PriceEvent> buffer = new ArrayList<>();
}
