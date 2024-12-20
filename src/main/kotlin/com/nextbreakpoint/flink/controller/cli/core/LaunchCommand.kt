package com.nextbreakpoint.flink.controller.cli.core

import com.nextbreakpoint.flink.controller.common.FlinkOptions

interface LaunchCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, args: T)
}

