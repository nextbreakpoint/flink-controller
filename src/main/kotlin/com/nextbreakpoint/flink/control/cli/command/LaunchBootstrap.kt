package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.LaunchCommand
import com.nextbreakpoint.flink.control.common.BootstrapOptions
import com.nextbreakpoint.flink.control.common.FlinkOptions
import com.nextbreakpoint.flink.control.k8s.bootstrap.BootstrapRunner
import com.nextbreakpoint.flink.control.k8s.common.FlinkClient
import com.nextbreakpoint.flink.control.k8s.common.KubeClient
import com.nextbreakpoint.flink.control.k8s.controller.Controller

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
