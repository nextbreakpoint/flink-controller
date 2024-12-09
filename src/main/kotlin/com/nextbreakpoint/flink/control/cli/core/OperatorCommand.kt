package com.nextbreakpoint.flink.control.cli.core

import com.nextbreakpoint.flink.control.common.ConnectionConfig

interface OperatorCommand {
    fun run(connectionConfig: ConnectionConfig)
}



