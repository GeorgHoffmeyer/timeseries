package org.gho.timeseries.service

import dto.ClusterDto
import dto.PointInTimeData
import org.gho.timeseries.repository.TimeseriesRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.max
import kotlin.math.min

@Service
class TimeseriesService(val timeseriesRepository: TimeseriesRepository) {
    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java.name)
    }

    fun getTimeseries(key: String): List<PointInTimeData> {
        return timeseriesRepository.getAll(key)
    }

    fun addPontInTimeData(key: String, pointInTimeData: PointInTimeData) {
        timeseriesRepository.add(key, pointInTimeData)
    }

    fun aggregatePointsInTime(key: String,
                              pointInTimeInterval: Long,
                              timestampStart: Long,
                              timestampEnd: Long,
                              agregateInterval: Long): Map<Long, BigDecimal> {

        val timeseries = timeseriesRepository.getAll(key).filter { pit ->
            (pit.timestamp - timestampStart).rem(pointInTimeInterval) == 0L
        }

        val agregate = mutableMapOf<Long, BigDecimal>()

        timeseries
                .filter { pit -> pit.timestamp in timestampStart..timestampEnd }
                .forEach {
                    val intervalNum = (it.timestamp - timestampStart).div(agregateInterval)

                    val currentSum = agregate[intervalNum] ?: BigDecimal.ZERO
                    val newSum = currentSum.add(it.value)

                    agregate[intervalNum] = newSum
                }

        return agregate.toMap()
    }

    fun clusterTimeseries(key: String,
                          pointInTimeInterval: Int,
                          timestampStart: Long,
                          timestampEnd: Long,
                          agregateInterval: Int): List<ClusterDto> {

        val clustredTimeseries = mutableListOf<ClusterDto>()

        timeseriesRepository.getAll(key)
                .filter { pit ->
                    (pit.timestamp - timestampStart).rem(pointInTimeInterval) == 0L
                }
                .filter { pit -> pit.timestamp in timestampStart..timestampEnd }
                .forEach { pit ->
                    val index = (pit.timestamp - timestampStart).div(agregateInterval)

                    if(clustredTimeseries.size > index.toInt()) {
                        val clusterDto = clustredTimeseries[index.toInt()]

                        val from = min(clusterDto.from, pit.timestamp)
                        val till = max(clusterDto.till, pit.timestamp)
                        val sum = clusterDto.sum.add(pit.value)
                        val max = maxOf(clusterDto.max, pit.value)
                        var maxTimestamp = clusterDto.maxTimestamp
                        if (max == pit.value) {
                            maxTimestamp = pit.timestamp
                        }
                        val min = minOf(clusterDto.min, pit.value)
                        var minTimestamp = clusterDto.minTimestamp
                        if (min == pit.value) {
                            minTimestamp = pit.timestamp
                        }
                        val itemCount = clusterDto.itemCount +1
                        val avg = sum.divide(BigDecimal(itemCount), 2, RoundingMode.HALF_UP)

                        clustredTimeseries[index.toInt()] = ClusterDto(from, till, sum, avg, max, min, maxTimestamp, minTimestamp, itemCount)
                    } else {

                        val from = pit.timestamp
                        val till = pit.timestamp
                        val sum = pit.value
                        val max = pit.value
                        var maxTimestamp = pit.timestamp
                        val min = pit.value
                        var minTimestamp = pit.timestamp
                        val itemCount = 1
                        val avg = pit.value

                        clustredTimeseries.add(index.toInt(), ClusterDto(from, till, sum, avg, max, min, maxTimestamp, minTimestamp, itemCount))
                    }
                }


        return clustredTimeseries
    }
}