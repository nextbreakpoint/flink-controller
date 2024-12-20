package com.nextbreakpoint.flink.controller.k8s.bootstrap

import com.nextbreakpoint.flink.controller.common.BootstrapOptions
import com.nextbreakpoint.flink.controller.k8s.controller.Controller

class BootstrapRunner(
    private val controller: Controller,
    private val namespace: String,
    private val options: BootstrapOptions
) {
    fun run() {
        val bootstrap = Bootstrap(controller, namespace, options)

        if (!options.dryRun) {
            bootstrap.run()
        }
    }
}