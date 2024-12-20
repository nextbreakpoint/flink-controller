package com.nextbreakpoint.flink.controller.cli.core

import com.nextbreakpoint.flink.controller.common.ConnectionConfig

interface JobCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: T)
}

