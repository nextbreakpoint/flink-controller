package com.nextbreakpoint.flink.control.common

enum class ClusterStatus {
    Unknown,
    Starting,
    Started,
    Stopping,
    Stopped,
    Terminated
}