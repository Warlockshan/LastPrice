package com.SP.LastPriceJava;

import com.SP.LastPriceJava.kafka.PriceBatchMessage;
import com.SP.LastPriceJava.model.PriceEvent;
import com.SP.LastPriceJava.service.InMemoryPriceStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "price-batch-topic")
@DirtiesContext
public class PriceBatchIntegrationTest {
    @Autowired
    KafkaTemplate<String, PriceBatchMessage> kafkaTemplate;

    @Autowired
    InMemoryPriceStore store;
    @Test
    void completedBatch_shouldExposeLastPrice() {

        String batchId = UUID.randomUUID().toString();

        send(batchId, PriceBatchMessage.Type.START, null);

        PriceEvent price = price("AAPL", "2024-01-01T10:00:00Z", 100);
        send(batchId, PriceBatchMessage.Type.CHUNK, List.of(price));

        send(batchId, PriceBatchMessage.Type.COMPLETE, null);

        await().atMost(5, SECONDS).untilAsserted(() -> {
            PriceEvent stored = store.getLastPrice("AAPL");
            assertThat(stored).isNotNull();
            assertThat(stored.payload.get("value")).isEqualTo(100);
        });
    }
    @Test
    void cancelledBatch_shouldNotExposeAnyPrice() {

        String batchId = UUID.randomUUID().toString();

        send(batchId, PriceBatchMessage.Type.START, null);

        PriceEvent price = price("GOOG", "2024-01-01T10:00:00Z", 200);
        send(batchId, PriceBatchMessage.Type.CHUNK, List.of(price));

        send(batchId, PriceBatchMessage.Type.CANCEL, null);

        await().atMost(3, SECONDS).untilAsserted(() -> {
            assertThat(store.getLastPrice("GOOG")).isNull();
        });
    }

    @Test
    void readBeforeComplete_shouldReturnNull() {

        String batchId = UUID.randomUUID().toString();

        send(batchId, PriceBatchMessage.Type.START, null);

        PriceEvent price = price("MSFT", "2024-01-01T10:00:00Z", 300);
        send(batchId, PriceBatchMessage.Type.CHUNK, List.of(price));

        // NO COMPLETE
        await().atMost(2, SECONDS).untilAsserted(() -> {
            assertThat(store.getLastPrice("MSFT")).isNull();
        });
    }

    @Test
    void laterAsOf_shouldWinEvenIfArrivesEarlier() {

        String batchId = UUID.randomUUID().toString();

        send(batchId, PriceBatchMessage.Type.START, null);

        PriceEvent newer = price("TSLA", "2024-01-01T12:00:00Z", 500);
        PriceEvent older = price("TSLA", "2024-01-01T10:00:00Z", 400);

        // send newer first
        send(batchId, PriceBatchMessage.Type.CHUNK, List.of(newer));
        send(batchId, PriceBatchMessage.Type.CHUNK, List.of(older));

        send(batchId, PriceBatchMessage.Type.COMPLETE, null);

        await().atMost(5, SECONDS).untilAsserted(() -> {
            PriceEvent stored = store.getLastPrice("TSLA");
            assertThat(stored.payload.get("value")).isEqualTo(500);
        });
    }

    private void send(String batchId,
                      PriceBatchMessage.Type type,
                      List<PriceEvent> events) {

        PriceBatchMessage msg = new PriceBatchMessage();
        msg.batchId = batchId;
        msg.type = type;
        msg.events = events;

        kafkaTemplate.send("price-batch-topic", batchId, msg);
    }

    private PriceEvent price(String id, String asOf, int value) {
        PriceEvent p = new PriceEvent();
        p.id = id;
        p.asOf = Instant.parse(asOf);
        p.payload = Map.of("value", value);
        return p;
    }
}