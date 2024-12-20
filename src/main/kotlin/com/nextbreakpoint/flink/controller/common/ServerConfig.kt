package com.nextbreakpoint.flink.controller.common

data class ServerConfig(
    val port: Int,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?
)