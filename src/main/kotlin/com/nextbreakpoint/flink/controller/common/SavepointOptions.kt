package com.nextbreakpoint.flink.controller.common

import com.fasterxml.jackson.annotation.JsonAutoDetect

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
data class SavepointOptions(
    val targetPath: String?
)