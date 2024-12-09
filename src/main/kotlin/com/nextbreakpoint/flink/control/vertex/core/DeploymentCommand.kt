package com.nextbreakpoint.flink.control.vertex.core

import com.nextbreakpoint.flink.control.common.DeploymentSelector

data class DeploymentCommand(val selector: DeploymentSelector, val json: String)