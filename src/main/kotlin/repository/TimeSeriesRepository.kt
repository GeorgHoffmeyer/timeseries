package repository

import dto.PointInTimeData
import java.math.BigDecimal

internal class TimeSeriesRepository {

    val timeSeries : MutableMap<String, MutableMap<Long, BigDecimal>>

    constructor() {
        timeSeries = mutableMapOf()
    }

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

        var value = timeserieseForKey.get(timestamp)
        if(value == null)
            return null

        return PointInTimeData(timestamp, value)
    }

    fun getAll(key:String) : List<PointInTimeData> {

        return timeSeries.getOrDefault(key, mutableMapOf<Long, BigDecimal>()).map { entry: Map.Entry<Long, BigDecimal> -> PointInTimeData(entry.key, entry.value) }
    }

    fun clear() {
        timeSeries.clear()
    }
}