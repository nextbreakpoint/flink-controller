package com.nextbreakpoint.flink.controller.k8s.common

class FlinkClientException(message: String, cause: Exception? = null): RuntimeException(message, cause)
