package com.nextbreakpoint.flink.controller.cli.core

import com.nextbreakpoint.flink.controller.common.ConnectionConfig

interface ClusterCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, args: T)
}

