package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.github.ajalt.clikt.parameters.types.int
import com.nextbreakpoint.command.*
import com.nextbreakpoint.model.*
import io.kubernetes.client.Configuration
import org.apache.log4j.Logger

class FlinkSubmitMain {
    companion object {
        val logger = Logger.getLogger(FlinkSubmitMain::class.simpleName)

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                FlinkSubmit().subcommands(
                    Create(),
                    Delete(),
                    Submit(),
                    Cancel(),
                    ListCommand(),
                    Server(),
                    FlinkSubmitSidecar().subcommands(
                        SidecarSubmit(),
                        SidecarWatch()
                    )
                ).main(args)
            } catch (e: Exception) {
                logger.error("An error occurred while launching the application: ${e.message}")
                System.exit(-1)
            }
        }
    }

    class FlinkSubmit: CliktCommand(name = "FlinkSubmit") {
        override fun run() = Unit
    }

    class FlinkSubmitSidecar: CliktCommand(name = "sidecar", help = "Sidecar commands") {
        override fun run() = Unit
    }

    class Create: CliktCommand(help="Create a cluster") {
        private val host: String by option(help="The FlinkSubmit server address").default("localhost")
        private val port: Int by option(help="The FlinkSubmit server port").int().default(4444)
        private val clusterName: String by option(help="The name of the new Flink cluster").required()
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val environment: String by option(help="The name of the environment").default("test")
        private val flinkImage: String by option(help="The image to use for JobManager and TaskManager").required()
        private val sidecarImage: String by option(help="The image to use for Flink Submit sidecar").required()
        private val sidecarArgument: List<String> by option(help="The argument for Flink Submit sidecar").multiple()
        private val sidecarArguments: String by option(help="The arguments for Flink Submit sidecar").default("")
        private val imagePullPolicy: String by option(help="The image pull policy").default("IfNotPresent")
        private val imagePullSecrets: String by option(help="The image pull secrets").required()
        private val jobmanagerCpus: Float by option(help="The JobManager's cpus limit").float().default(1f)
        private val taskmanagerCpus: Float by option(help="The TaskManager's cpus limit").float().default(1f)
        private val jobmanagerMemory: Int by option(help="The JobManager's memory limit in Mb").int().default(512)
        private val taskmanageMemory: Int by option(help="The TaskManager's memory limit in Mb").int().default(1024)
        private val jobmanagerStorageSize: Int by option(help="The JobManager's storage size in Gb").int().default(2)
        private val taskmanagerStorageSize: Int by option(help="The TaskManager's storage size in Gb").int().default(5)
        private val jobmanagerStorageClass: String by option(help="The JobManager's storage class").default("standard")
        private val taskmanagerStorageClass: String by option(help="The TaskManager's storage class").default("standard")
        private val taskmanagerTaskSlots: Int by option(help="The number of task slots for each TaskManager").int().default(1)
        private val taskmanagerReplicas: Int by option(help="The number of TaskManager replicas").int().default(1)
        private val jobmanagerServiceMode: String by option(help="The JobManager's service type").default("clusterIP")

        override fun run() {
            val config = ClusterConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jobmanager = JobManagerConfig(
                    image = flinkImage,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    serviceMode = jobmanagerServiceMode,
                    storage = StorageConfig(
                        size = jobmanagerStorageSize,
                        storageClass = jobmanagerStorageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = jobmanagerCpus,
                        memory = jobmanagerMemory
                    )
                ),
                taskmanager = TaskManagerConfig(
                    image = flinkImage,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    taskSlots = taskmanagerTaskSlots,
                    replicas = taskmanagerReplicas,
                    storage = StorageConfig(
                        size = taskmanagerStorageSize,
                        storageClass = taskmanagerStorageClass
                    ),
                    resources = ResourcesConfig(
                        cpus = taskmanagerCpus,
                        memory = taskmanageMemory
                    )
                ),
                sidecar = SidecarConfig(
                    image = sidecarImage,
                    pullPolicy = imagePullPolicy,
                    pullSecrets = imagePullSecrets,
                    arguments = if (sidecarArguments.isNotBlank()) sidecarArguments else sidecarArgument.joinToString(" ")
                )
            )
            CreateCluster().run(ApiConfig(host, port), config)
            System.exit(0)
        }
    }

    class Delete: CliktCommand(help="Delete a cluster") {
        private val host: String by option(help="The FlinkSubmit server address").default("localhost")
        private val port: Int by option(help="The FlinkSubmit server port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val descriptor = ClusterDescriptor(
                namespace = namespace,
                name = clusterName,
                environment = environment
            )
            DeleteCluster().run(ApiConfig(host, port), descriptor)
            System.exit(0)
        }
    }

    class Submit: CliktCommand(help="Submit a job") {
        private val host: String by option(help="The FlinkSubmit server address").default("localhost")
        private val port: Int by option(help="The FlinkSubmit server port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val className: String? by option(help="The name of the class to submit")
        private val jarPath: String by option(help="The path of the jar to submit").required()
        private val arguments: String by option(help="The job's arguments (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").default("")
        private val argument: List<String> by option(help="The job's argument (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").multiple()
        private val fromSavepoint: String? by option(help="Resume the job from the savepoint")
        private val parallelism: Int by option(help="The parallelism of the job").int().default(1)

        override fun run() {
            val config = JobSubmitConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jarPath = jarPath,
                className = className,
                arguments = if (arguments.isNotBlank()) arguments else argument.joinToString(" "),
                savepoint = fromSavepoint,
                parallelism = parallelism
            )
            SubmitJob().run(ApiConfig(host, port), config)
            System.exit(0)
        }
    }

    class Cancel: CliktCommand(help="Cancel a job") {
        private val host: String by option(help="The FlinkSubmit server address").default("localhost")
        private val port: Int by option(help="The FlinkSubmit server port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val createSavepoint: Boolean by option(help="Create savepoint before stopping the job").flag(default = false)
        private val savepointPath: String by option(help="Directory where to save savepoint").default("file:///var/tmp/savepoints")
        private val jobId: String by option(help="The id of the job to cancel").prompt("Insert job id")

        override fun run() {
            val config = JobCancelConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                savepoint = createSavepoint,
                savepointPath = savepointPath,
                jobId = jobId
            )
            CancelJob().run(ApiConfig(host, port), config)
            System.exit(0)
        }
    }

    class ListCommand: CliktCommand(name="list", help="List jobs") {
        private val host: String by option(help="The FlinkSubmit server address").default("localhost")
        private val port: Int by option(help="The FlinkSubmit server port").int().default(4444)
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val onlyRunning: Boolean by option(help="List only running jobs").flag(default = true)

        override fun run() {
            val config = JobListConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                running = onlyRunning
            )
            ListJobs().run(ApiConfig(host, port), config)
            System.exit(0)
        }
    }

    class Server: CliktCommand(help="Run the server") {
        private val port: Int by option(help="Listen on port").int().default(4444)
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of Kubectl config")

        override fun run() {
            val config = ServerConfig(
                port = port,
                portForward = portForward,
                kubeConfig = kubeConfig
            )
            RunServer().run(config)
        }
    }

    class SidecarSubmit: CliktCommand(name="submit", help="Run submit command from sidecar") {
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")
        private val className: String? by option(help="The name of the class to submit")
        private val jarPath: String by option(help="The path of the jar to submit").required()
        private val arguments: String by option(help="The job's arguments (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").default("")
        private val argument: List<String> by option(help="The job's argument (\"--PARAM1 VALUE1 --PARAM2 VALUE2\")").multiple()
        private val fromSavepoint: String? by option(help="Resume the job from the savepoint")
        private val parallelism: Int by option(help="The parallelism of the job").int().default(1)

        override fun run() {
            val config = JobSubmitConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                ),
                jarPath = jarPath,
                className = className,
                arguments = if (arguments.isNotBlank()) arguments else argument.joinToString(" "),
                savepoint = fromSavepoint,
                parallelism = parallelism
            )
            Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))
            RunSidecarSubmit().run(portForward, kubeConfig != null, config)
        }
    }

    class SidecarWatch: CliktCommand(name="watch", help="Run watch command from sidecar") {
        private val portForward: Int? by option(help="Connect to JobManager using port forward").int()
        private val kubeConfig: String? by option(help="The path of kuke config")
        private val namespace: String by option(help="The namespace where to create the resources").default("default")
        private val clusterName: String by option(help="The name of the Flink cluster").required()
        private val environment: String by option(help="The name of the environment").default("test")

        override fun run() {
            val config = WatchConfig(
                descriptor = ClusterDescriptor(
                    namespace = namespace,
                    name = clusterName,
                    environment = environment
                )
            )
            Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))
            RunSidecarWatch().run(portForward, kubeConfig != null, config)
        }
    }
}