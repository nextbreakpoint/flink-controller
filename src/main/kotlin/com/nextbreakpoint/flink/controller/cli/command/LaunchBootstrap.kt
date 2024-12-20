package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.LaunchCommand
import com.nextbreakpoint.flink.controller.common.BootstrapOptions
import com.nextbreakpoint.flink.controller.common.FlinkOptions
import com.nextbreakpoint.flink.controller.k8s.bootstrap.BootstrapRunner
import com.nextbreakpoint.flink.controller.k8s.common.FlinkClient
import com.nextbreakpoint.flink.controller.k8s.common.KubeClient
import com.nextbreakpoint.flink.controller.k8s.controller.Controller

class LaunchBootstrap : LaunchCommand<BootstrapOptions> {
    companion object {
        private val kubeClient = KubeClient
        private val flinkClient = FlinkClient
    }

    override fun run(flinkOptions: FlinkOptions, namespace: String, options: BootstrapOptions) {
        val controller = Controller(flinkOptions, flinkClient, kubeClient, options.dryRun)

        val runner = BootstrapRunner(controller, namespace, options)

        runner.run()
    }
}
