package com.nextbreakpoint.flink.control.k8s.common

class FlinkClientException(message: String, cause: Exception? = null): RuntimeException(message, cause)
