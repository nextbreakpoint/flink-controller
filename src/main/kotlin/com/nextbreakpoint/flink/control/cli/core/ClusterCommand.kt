package com.nextbreakpoint.flink.control.cli.core

import com.nextbreakpoint.flink.control.common.ConnectionConfig

interface ClusterCommand<T> {
    fun run(connectionConfig: ConnectionConfig, clusterName: String, args: T)
}

