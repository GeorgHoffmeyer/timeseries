package org.gho.timeseries

import org.gho.timeseries.handler.TimerseriesHandler
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse

@EnableConfigurationProperties
@SpringBootApplication
class TimerseriesServiceApplication {

    @Bean
    fun routerFunctionTimerseries(timerseriesHandler: TimerseriesHandler): RouterFunction<ServerResponse> {

        return RouterFunctions.route().GET("/timeseries", timerseriesHandler::getTimeseries).build()
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(TimerseriesServiceApplication::class.java, *args)
}