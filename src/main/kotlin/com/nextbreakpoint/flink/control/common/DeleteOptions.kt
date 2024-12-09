package com.nextbreakpoint.flink.control.common

data class DeleteOptions(
    val label: String,
    val value: String,
    val limit: Int
)