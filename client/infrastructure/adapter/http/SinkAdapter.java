package com.stream.client.infrastructure.adapter.http;

import com.stream.client.domain.port.SinkPort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import com.stream.client.domain.model.Record;

@Component
@RequiredArgsConstructor
public class SinkAdapter implements SinkPort {

    private final WebClient webClient;

    @Override
    public Mono<Void> sendRecord(Record record) {
        return webClient.post()
                .uri("/sink/a")
                .bodyValue(record)
                .retrieve()
                .bodyToMono(Void.class);
    }
}
