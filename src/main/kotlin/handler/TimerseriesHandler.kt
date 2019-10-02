package org.gho.timeseries.handler

import org.gho.timeseries.service.TimeseriesService
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

@Component
class TimerseriesHandler(val trimerseriesService: TimeseriesService) {

    fun getTimeseries(request: ServerRequest): Mono<ServerResponse> {

        return ServerResponse.ok().body(BodyInserters.fromObject("OK"))
    }
}