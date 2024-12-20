package com.nextbreakpoint.flink.controller.vertex.core

import com.nextbreakpoint.flink.controller.common.DeploymentSelector

data class DeploymentCommand(val selector: DeploymentSelector, val json: String)