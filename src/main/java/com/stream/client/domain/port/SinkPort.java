package com.stream.client.domain.port;

import reactor.core.publisher.Mono;
import com.stream.client.domain.model.Record;

public interface SinkPort {
    Mono<Void> sendRecord(Record record);
}
