package com.stream.client.application;

import com.stream.client.domain.model.Record;
import com.stream.client.domain.port.SinkPort;
import com.stream.client.domain.port.SourcePort;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StreamClientService {

    private final SourcePort sourceA;
    private final SourcePort sourceB;
    private final SinkPort sink;

    public StreamClientService(@Qualifier("sourceAAdapter") SourcePort sourceA,
                               @Qualifier("sourceBAdapter") SourcePort sourceB,
                               SinkPort sink) {
        this.sourceA = sourceA;
        this.sourceB = sourceB;
        this.sink = sink;
    }

    private static final long ORPHAN_TIMEOUT_MS = 60000; // 15 seconds

    // Unified pending map: ID → (source, timestamp)
    private final Map<String, PendingEntry> pendingMap = new ConcurrentHashMap<>();

    private record PendingEntry(String source, long timestamp) {}

    @PostConstruct
    public void start() {
        log.info("Starting improved reactive streaming client");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            flushAllPending().blockLast(Duration.ofSeconds(10));
        }));

        Flux.merge(streamA(), streamB(), orphanFlusher())
                .subscribe(
                        null,
                        e -> log.error("Stream error: {}", e.getMessage()),
                        () -> log.info("Streaming finished and all pending records flushed")
                );
    }

    private Flux<Void> streamA() {
        return sourceA.streamRecords()
                .flatMap(line -> {
                    try {
                        if (line.contains("\"status\": \"ok\"")) {
                            String id = line.split("\"id\": \"")[1].split("\"")[0];
                            return handleIncoming("A", id);
                        }
                    } catch (Exception e) {
                        log.warn("Malformed A record: {}", line);
                    }
                    return Flux.empty();
                });
    }

    private Flux<Void> streamB() {
        return sourceB.streamRecords()
                .flatMap(line -> {
                    try {
                        if (!line.contains("<done/>")) {
                            String id = line.split("value=\"")[1].split("\"")[0];
                            return handleIncoming("B", id);
                        }
                    } catch (Exception e) {
                        log.warn("Malformed B record: {}", line);
                    }
                    return Flux.empty();
                });
    }

    /**
     * Atomically handle a record — if opposite source exists, send joined; otherwise store.
     */
    private Flux<Void> handleIncoming(String source, String id) {
        return Flux.defer(() -> {
            boolean shouldSendJoined = pendingMap.compute(id, (key, existing) -> {
                if (existing == null) {
                    // store as pending
                    return new PendingEntry(source, System.currentTimeMillis());
                }
                // if opposite, remove and send joined
                if (!existing.source.equals(source)) {
                    return null;
                }
                // same source duplicate → keep original timestamp
                return existing;
            }) == null; // null return means we matched and removed

            return shouldSendJoined ? sendJoinedFlux(id) : Flux.empty();
        });
    }

    private Flux<Void> sendJoinedFlux(String id) {
        return sink.sendRecord(new Record("joined", id))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)))
                .onErrorResume(e -> {
                    log.warn("Failed to send joined {}: {}", id, e.getMessage());
                    return Mono.empty();
                })
                .flux();
    }

    private Flux<Void> sendOrphanFlux(String id) {
        return sink.sendRecord(new Record("orphaned", id))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)))
                .onErrorResume(e -> {
                    log.warn("Failed to send orphan {}: {}", id, e.getMessage());
                    return Mono.empty();
                })
                .flux();
    }

    /**
     * Periodically flush old pending items as orphans.
     */
    private Flux<Void> orphanFlusher() {
        return Flux.interval(Duration.ofSeconds(2))
                .flatMap(tick -> flushExpired());
    }

    private Flux<Void> flushExpired() {
        long now = System.currentTimeMillis();

        List<String> expiredIds = pendingMap.entrySet().stream()
                .filter(e -> now - e.getValue().timestamp >= ORPHAN_TIMEOUT_MS)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        expiredIds.forEach(pendingMap::remove);

        if (expiredIds.isEmpty()) return Flux.empty();

        return Flux.fromIterable(expiredIds)
                .flatMap(this::sendOrphanFlux, 64); // small concurrency
    }

    /**
     * Flush everything as orphans on shutdown.
     */
    private Flux<Void> flushAllPending() {
        List<String> remaining = List.copyOf(pendingMap.keySet());
        pendingMap.clear();
        if (remaining.isEmpty()) return Flux.empty();

        return Flux.fromIterable(remaining)
                .flatMap(this::sendOrphanFlux, 64);
    }
}
