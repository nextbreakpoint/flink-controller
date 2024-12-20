package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.DeploymentCommand
import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig

class DeploymentStatus(private val factory: WebClientFactory = WebClientDefaultFactory) : DeploymentCommand<Void?> {
    override fun run(connectionConfig: ConnectionConfig, deploymentName: String, unused: Void?) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/deployments/$deploymentName/status")
    }
}

