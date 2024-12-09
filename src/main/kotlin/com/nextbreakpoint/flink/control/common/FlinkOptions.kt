package com.nextbreakpoint.flink.control.common

data class FlinkOptions(
    val hostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean
)