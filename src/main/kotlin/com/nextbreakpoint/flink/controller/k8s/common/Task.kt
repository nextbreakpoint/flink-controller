package com.nextbreakpoint.flink.controller.k8s.common

abstract class Task<T> {
    abstract fun execute(target: T)
}