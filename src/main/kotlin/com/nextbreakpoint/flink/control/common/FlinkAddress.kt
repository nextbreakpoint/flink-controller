package com.nextbreakpoint.flink.control.common

data class FlinkAddress(val host: String, val port: Int, val protocol: String? = "http")