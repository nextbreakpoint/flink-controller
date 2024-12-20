package com.nextbreakpoint.flink.controller.common

enum class ClusterStatus {
    Unknown,
    Starting,
    Started,
    Stopping,
    Stopped,
    Terminated
}