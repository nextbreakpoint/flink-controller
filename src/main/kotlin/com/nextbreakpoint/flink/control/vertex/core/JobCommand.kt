package com.nextbreakpoint.flink.control.vertex.core

import com.nextbreakpoint.flink.control.common.JobSelector

data class JobCommand(val selector: JobSelector, val json: String)