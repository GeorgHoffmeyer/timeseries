package service

import dto.PointInTimeData
import org.gho.timeseries.service.TimeseriesService
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import repository.TimeSeriesRepository
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.ZoneOffset

//import org.junit.jupiter.api.Assertions.*

internal class TimeseriesServiceTest {

    var timeSeriesRepository = TimeSeriesRepository()
    var timeseriesService = TimeseriesService(timeSeriesRepository)

    @Before
    fun initTest() {
        timeSeriesRepository.clear()
    }

    @Test
    fun testAddPointInTimeData() {
        val dataNow = LocalDateTime.now()

        val key = "timeseries1"

        var pointInTimeData1 = PointInTimeData(dataNow.toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        var pointInTimeData2 = PointInTimeData(dataNow.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        var pointInTimeData3 = PointInTimeData(dataNow.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        var pointInTimeData4 = PointInTimeData(dataNow.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)

        Assert.assertEquals(4, timeseriesService.getTimeseries(key).size)
    }

    @Test
    fun testAddPointInTimeTwice() {
        val dataNow = LocalDateTime.now().plusHours(1)

        val key = "timeseries2"

        val pointInTimeData1 = PointInTimeData(dataNow.toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData2 = PointInTimeData(dataNow.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData3 = PointInTimeData(dataNow.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData4 = PointInTimeData(dataNow.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData5 = PointInTimeData(dataNow.toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData6 = PointInTimeData(dataNow.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData7 = PointInTimeData(dataNow.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData8 = PointInTimeData(dataNow.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)
        timeseriesService.addPontInTimeData(key, pointInTimeData5)
        timeseriesService.addPontInTimeData(key, pointInTimeData6)
        timeseriesService.addPontInTimeData(key, pointInTimeData7)
        timeseriesService.addPontInTimeData(key, pointInTimeData8)

        Assert.assertEquals(4, timeseriesService.getTimeseries(key).size)

    }

    @Test
    fun testMissingKey() {
        val dateNow = LocalDateTime.now()

        val key = "timeseries1"
        val key2 = "timeseries2"

        val pointInTimeData1 = PointInTimeData(dateNow.toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData2 = PointInTimeData(dateNow.plusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData3 = PointInTimeData(dateNow.plusMinutes(2).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData4 = PointInTimeData(dateNow.plusMinutes(3).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)

        Assert.assertEquals(0, timeseriesService.getTimeseries(key2).size)
    }

    @Test
    fun testAggregation() {
        val intervalInSeconds = 30L
        val key = "timeseries1"
        val dateNow = LocalDateTime.now()

        val pointInTimeData1 = PointInTimeData(dateNow.plusSeconds(intervalInSeconds * 0).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData2 = PointInTimeData(dateNow.plusSeconds(intervalInSeconds * 1).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData3 = PointInTimeData(dateNow.plusSeconds(intervalInSeconds * 2).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)
        val pointInTimeData4 = PointInTimeData(dateNow.plusSeconds(intervalInSeconds * 3).toInstant(ZoneOffset.UTC).toEpochMilli(), BigDecimal.ONE)

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)

        val aggrgation = timeseriesService.aggregatePointsInTime(key,
                intervalInSeconds * 1000,
                dateNow.toInstant(ZoneOffset.UTC).toEpochMilli(),
                dateNow.plusSeconds(intervalInSeconds * 3).toInstant(ZoneOffset.UTC).toEpochMilli(),
                intervalInSeconds * 4 * 1000)

        Assert.assertEquals(1, aggrgation.size)
        Assert.assertEquals(BigDecimal.valueOf(4), aggrgation[0])

    }

    @Test
    fun testAggregation2() {
        val key = "timeseries1"
        val interval = 1L
        val startPoint = 10L

        val pointInTimeData1 = PointInTimeData(startPoint + 0, BigDecimal.valueOf(1))
        val pointInTimeData2 = PointInTimeData(startPoint + 1, BigDecimal.valueOf(2))
        val pointInTimeData3 = PointInTimeData(startPoint + 2, BigDecimal.valueOf(3))
        val pointInTimeData4 = PointInTimeData(startPoint + 3, BigDecimal.valueOf(4))
        val pointInTimeData5 = PointInTimeData(startPoint + 4, BigDecimal.valueOf(5))
        val pointInTimeData6 = PointInTimeData(startPoint + 5, BigDecimal.valueOf(6))
        val pointInTimeData7 = PointInTimeData(startPoint + 6, BigDecimal.valueOf(7))
        val pointInTimeData8 = PointInTimeData(startPoint + 7, BigDecimal.valueOf(8))

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)
        timeseriesService.addPontInTimeData(key, pointInTimeData5)
        timeseriesService.addPontInTimeData(key, pointInTimeData6)
        timeseriesService.addPontInTimeData(key, pointInTimeData7)
        timeseriesService.addPontInTimeData(key, pointInTimeData8)

        val aggregation = timeseriesService.aggregatePointsInTime(key, interval, 10L, 17L, 4)

        Assert.assertEquals(2, aggregation.size)
        Assert.assertEquals(BigDecimal.valueOf(10), aggregation[0])
        Assert.assertEquals(BigDecimal.valueOf(26), aggregation[1])
    }

    @Test
    fun testClustrer() {
        val key = "timeseries1"
        val interval = 1
        val startPoint = 10L

        val pointInTimeData1 = PointInTimeData(startPoint + 0, BigDecimal.valueOf(1))
        val pointInTimeData2 = PointInTimeData(startPoint + 1, BigDecimal.valueOf(2))
        val pointInTimeData3 = PointInTimeData(startPoint + 2, BigDecimal.valueOf(3))
        val pointInTimeData4 = PointInTimeData(startPoint + 3, BigDecimal.valueOf(4))
        val pointInTimeData5 = PointInTimeData(startPoint + 4, BigDecimal.valueOf(5))
        val pointInTimeData6 = PointInTimeData(startPoint + 5, BigDecimal.valueOf(6))
        val pointInTimeData7 = PointInTimeData(startPoint + 6, BigDecimal.valueOf(7))
        val pointInTimeData8 = PointInTimeData(startPoint + 7, BigDecimal.valueOf(8))

        timeseriesService.addPontInTimeData(key, pointInTimeData1)
        timeseriesService.addPontInTimeData(key, pointInTimeData2)
        timeseriesService.addPontInTimeData(key, pointInTimeData3)
        timeseriesService.addPontInTimeData(key, pointInTimeData4)
        timeseriesService.addPontInTimeData(key, pointInTimeData5)
        timeseriesService.addPontInTimeData(key, pointInTimeData6)
        timeseriesService.addPontInTimeData(key, pointInTimeData7)
        timeseriesService.addPontInTimeData(key, pointInTimeData8)

        val clusteredData = timeseriesService.clusterTimeseries(key, interval, 10L, 17L, 4)

        Assert.assertEquals(2, clusteredData.size)

        Assert.assertEquals(4, clusteredData[0].itemCount)
        Assert.assertEquals(10L, clusteredData[0].from)
        Assert.assertEquals(13L, clusteredData[0].till)
        Assert.assertEquals(BigDecimal(10), clusteredData[0].sum)
        Assert.assertEquals(10L, clusteredData[0].minTimestamp)
        Assert.assertEquals(BigDecimal(1), clusteredData[0].min)
        Assert.assertEquals(13L, clusteredData[0].maxTimestamp)
        Assert.assertEquals(BigDecimal(4), clusteredData[0].max)
        Assert.assertEquals(BigDecimal.valueOf(250, 2), clusteredData[0].avg)

        Assert.assertEquals(4, clusteredData[1].itemCount)
        Assert.assertEquals(14L, clusteredData[1].from)
        Assert.assertEquals(17L, clusteredData[1].till)
        Assert.assertEquals(BigDecimal(26), clusteredData[1].sum)
        Assert.assertEquals(14L, clusteredData[1].minTimestamp)
        Assert.assertEquals(BigDecimal(5), clusteredData[1].min)
        Assert.assertEquals(17L, clusteredData[1].maxTimestamp)
        Assert.assertEquals(BigDecimal(8), clusteredData[1].max)
        Assert.assertEquals(BigDecimal.valueOf(400, 2), clusteredData[1].avg)
    }

    @Test
    fun testMany() {
        val key = "timeseries1"
        val interval = 1
        val startPoint = 10L
        val amount = 1000000
        val agregationInterval = 96

        for(i in 0..amount) {
            val pointInTimeData = PointInTimeData(startPoint + i, BigDecimal.valueOf(1))

            timeseriesService.addPontInTimeData(key, pointInTimeData)
        }

        println("Timeseries count: ${timeseriesService.getTimeseries(key).size}")

        val currentTimeBefore = System.nanoTime()
        val clusteredData = timeseriesService.clusterTimeseries(key, interval, 10L, amount+startPoint, agregationInterval)
        val currentTimeAfter = System.nanoTime()

        val durationInMilliseconds = (currentTimeAfter - currentTimeBefore)/1000000

        println("Took $durationInMilliseconds millliseconds");
        println("Cluster Count ${clusteredData.size}")
    }
}