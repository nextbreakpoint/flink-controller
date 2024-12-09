package com.nextbreakpoint.flink.control.vertex.core

import com.nextbreakpoint.flink.control.common.ClusterSelector

data class ClusterCommand(val selector: ClusterSelector, val json: String)