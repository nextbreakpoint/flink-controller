package com.nextbreakpoint.flink.controller.cli.core

import com.nextbreakpoint.flink.controller.common.ConnectionConfig

interface DeploymentCommand<T> {
    fun run(connectionConfig: ConnectionConfig, deploymentName: String, args: T)
}

