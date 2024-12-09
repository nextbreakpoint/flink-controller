package com.nextbreakpoint.flink.control.common

data class RunJarOptions(
    val jarFileId: String,
    val className: String,
    val parallelism: Int,
    val savepointPath: String?,
    val arguments: List<String>
)