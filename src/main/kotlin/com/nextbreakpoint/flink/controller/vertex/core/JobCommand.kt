package com.nextbreakpoint.flink.controller.vertex.core

import com.nextbreakpoint.flink.controller.common.JobSelector

data class JobCommand(val selector: JobSelector, val json: String)