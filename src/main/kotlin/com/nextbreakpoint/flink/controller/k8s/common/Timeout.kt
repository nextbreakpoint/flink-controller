package com.nextbreakpoint.flink.controller.k8s.common

object Timeout {
    val TASK_TIMEOUT = System.getenv("TASK_TIMEOUT")?.toLong() ?: 60L
}
