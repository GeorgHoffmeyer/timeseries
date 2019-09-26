package dto

import java.math.BigDecimal

data class PointInTimeData(
        var timestamp : Long,
        var value : BigDecimal
)
