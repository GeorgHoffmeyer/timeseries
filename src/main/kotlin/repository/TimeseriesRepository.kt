package org.gho.timeseries.repository

import dto.PointInTimeData
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class TimeseriesRepository {

    val timeSeries : MutableMap<String, MutableMap<Long, BigDecimal>> = mutableMapOf()

    fun add(key : String, pointInTimeData : PointInTimeData) {
        if(timeSeries.get(key)!=null) {
            timeSeries.get(key)?.put(pointInTimeData.timestamp, pointInTimeData.value);
        } else {
            timeSeries.put(key, mutableMapOf(Pair(pointInTimeData.timestamp, pointInTimeData.value)))
        }
    }

    fun get(key : String, timestamp: Long) : PointInTimeData? {
        var timeserieseForKey = timeSeries.get(key)
        if(timeserieseForKey == null)
            return null

        var value: BigDecimal = timeserieseForKey.get(timestamp) ?: return null

        return PointInTimeData(timestamp, value)
    }

    fun getAll(key:String) : List<PointInTimeData> {

        return timeSeries.getOrDefault(key, mutableMapOf<Long, BigDecimal>()).map { entry: Map.Entry<Long, BigDecimal> -> PointInTimeData(entry.key, entry.value) }
    }

    fun clear() {
        timeSeries.clear()
    }
}