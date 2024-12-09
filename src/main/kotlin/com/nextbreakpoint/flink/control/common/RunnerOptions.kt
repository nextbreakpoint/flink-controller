package com.nextbreakpoint.flink.control.common

data class RunnerOptions(
    val pollingInterval: Long,
    val taskTimeout: Long,
    val serverConfig: ServerConfig
)