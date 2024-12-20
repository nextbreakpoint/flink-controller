package com.nextbreakpoint.flink.controller.common

data class ConnectionConfig(
    val host: String,
    val port: Int,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?
)