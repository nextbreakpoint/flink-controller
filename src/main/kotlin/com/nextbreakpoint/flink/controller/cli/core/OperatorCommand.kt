package com.nextbreakpoint.flink.controller.cli.core

import com.nextbreakpoint.flink.controller.common.ConnectionConfig

interface OperatorCommand {
    fun run(connectionConfig: ConnectionConfig)
}



