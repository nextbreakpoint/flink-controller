package com.nextbreakpoint.flink.controller.k8s.common

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

object Time {
    fun toDateTime(timestamp: Long) = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"))
}