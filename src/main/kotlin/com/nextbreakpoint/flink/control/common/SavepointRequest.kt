package com.nextbreakpoint.flink.control.common

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)
