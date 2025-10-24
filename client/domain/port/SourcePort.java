package com.stream.client.domain.port;

import reactor.core.publisher.Flux;

public interface SourcePort {
    Flux<String> streamRecords();
}
