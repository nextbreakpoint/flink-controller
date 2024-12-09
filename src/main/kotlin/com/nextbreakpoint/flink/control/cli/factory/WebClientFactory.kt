package com.nextbreakpoint.flink.control.cli.factory

import com.nextbreakpoint.flink.control.common.ConnectionConfig
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(connectionConfig: ConnectionConfig): WebClient
}
