package com.nextbreakpoint.flink.k8s.common

class FlinkClientException(message: String, cause: Exception? = null): RuntimeException(message, cause)
