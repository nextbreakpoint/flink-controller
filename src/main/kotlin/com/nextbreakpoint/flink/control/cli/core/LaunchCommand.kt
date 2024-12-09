package com.nextbreakpoint.flink.control.cli.core

import com.nextbreakpoint.flink.control.common.FlinkOptions

interface LaunchCommand<T> {
    fun run(flinkOptions: FlinkOptions, namespace: String, args: T)
}

