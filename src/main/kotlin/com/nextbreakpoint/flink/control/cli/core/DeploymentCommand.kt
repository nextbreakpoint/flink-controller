package com.nextbreakpoint.flink.control.cli.core

import com.nextbreakpoint.flink.control.common.ConnectionConfig

interface DeploymentCommand<T> {
    fun run(connectionConfig: ConnectionConfig, deploymentName: String, args: T)
}

