package repository

import dto.TimePoint

class TimeSeriesRepository {

    val timeSeries : MutableList<TimePoint>

    constructor() {
        timeSeries = mutableListOf()
    }

    fun add(timePoint : TimePoint) {
        timeSeries.add(timePoint)
    }

    fun get(timestamp : Int) : TimePoint? {
        return timeSeries.find { timePoint -> timePoint.timestamp.equals(timestamp)
        }
    }

    fun getAll() : List<TimePoint> {
        return timeSeries.filterNotNull()
    }
}