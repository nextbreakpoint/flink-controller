package com.nextbreakpoint.flink.controller.common

data class RunnerOptions(
    val pollingInterval: Long,
    val taskTimeout: Long,
    val serverConfig: ServerConfig
)