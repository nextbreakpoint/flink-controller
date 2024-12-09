package com.nextbreakpoint.flink.control.k8s.common

abstract class Task<T> {
    abstract fun execute(target: T)
}