package com.nextbreakpoint.flink.controller.common

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)
