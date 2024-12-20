package com.nextbreakpoint.flink.controller.common

data class FlinkAddress(val host: String, val port: Int, val protocol: String? = "http")