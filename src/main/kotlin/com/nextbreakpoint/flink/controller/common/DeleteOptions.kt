package com.nextbreakpoint.flink.controller.common

data class DeleteOptions(
    val label: String,
    val value: String,
    val limit: Int
)