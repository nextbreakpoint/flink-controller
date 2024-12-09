package com.nextbreakpoint.flink.control.cli.core

import com.nextbreakpoint.flink.control.common.ConnectionConfig

interface JobCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: T)
}

