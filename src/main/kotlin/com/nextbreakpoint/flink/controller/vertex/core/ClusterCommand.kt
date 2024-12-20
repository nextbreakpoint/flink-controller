package com.nextbreakpoint.flink.controller.vertex.core

import com.nextbreakpoint.flink.controller.common.ClusterSelector

data class ClusterCommand(val selector: ClusterSelector, val json: String)