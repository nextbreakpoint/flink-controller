package com.nextbreakpoint.flink.controller.cli.factory

import com.nextbreakpoint.flink.controller.common.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(connectionConfig: ConnectionConfig): WebClient
}
