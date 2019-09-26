package dto

import java.math.BigDecimal

data class ClusterDto(
    val from : Long,
    var till : Long,
    val sum : BigDecimal,
    val avg : BigDecimal,
    val max : BigDecimal,
    val min : BigDecimal,
    val maxTimestamp : Long,
    val minTimestamp : Long,
    val itemCount : Int
)