package com.stream.client.infrastructure.adapter.http;

import com.stream.client.domain.port.SourcePort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component("sourceBAdapter")
@RequiredArgsConstructor
public class SourceBAdapter implements SourcePort {

    private final WebClient webClient;

    public Flux<String> streamRecords() {
        return Flux.defer(() ->
                        webClient.get()
                                .uri("/source/b")
                                .retrieve()
                                .bodyToMono(String.class)
                                .onErrorResume(e -> Mono.empty())
                )
                .repeat() // keeps calling until completion
                .delayElements(Duration.ofMillis(1)) // short delay between requests
                .takeUntil(response -> response.contains("nothing else at the moment"));
    }
}
