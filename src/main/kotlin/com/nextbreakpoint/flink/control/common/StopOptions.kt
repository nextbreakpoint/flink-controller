package com.nextbreakpoint.flink.control.common

import com.fasterxml.jackson.annotation.JsonAutoDetect

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
data class StopOptions(
    val withoutSavepoint: Boolean
)