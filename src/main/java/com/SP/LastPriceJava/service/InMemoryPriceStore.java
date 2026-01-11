package com.SP.LastPriceJava.service;

import com.SP.LastPriceJava.model.PriceEvent;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InMemoryPriceStore {

    private final Map<String, PriceEvent> store = new ConcurrentHashMap<>();

    public void materialize(Iterable<PriceEvent> events) {
        for (PriceEvent event : events) {
            store.compute(event.id, (k, existing) ->
                    existing == null || event.asOf.isAfter(existing.asOf)
                            ? event
                            : existing
            );
        }
    }

    public PriceEvent getLastPrice(String id) {
        return store.get(id);
    }

}
