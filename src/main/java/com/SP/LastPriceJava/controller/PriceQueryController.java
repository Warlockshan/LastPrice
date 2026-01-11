package com.SP.LastPriceJava.controller;

import com.SP.LastPriceJava.model.PriceEvent;
import com.SP.LastPriceJava.service.InMemoryPriceStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/prices")
public class PriceQueryController {
    private final InMemoryPriceStore store;

    public PriceQueryController(InMemoryPriceStore store) {
        this.store = store;
    }

    @GetMapping("/{id}")
    public PriceEvent getLast(@PathVariable String id) {
        return store.getLastPrice(id);
    }
}
