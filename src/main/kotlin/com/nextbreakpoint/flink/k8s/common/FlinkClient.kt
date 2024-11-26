package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.RescaleStatus
import com.nextbreakpoint.flink.common.SavepointStatus
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.client.api.FlinkApi
import com.nextbreakpoint.flink.client.model.AsynchronousOperationInfo
import com.nextbreakpoint.flink.client.model.CheckpointingStatistics
import com.nextbreakpoint.flink.client.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flink.client.model.Id
import com.nextbreakpoint.flink.client.model.JarFileInfo
import com.nextbreakpoint.flink.client.model.JarRunResponseBody
import com.nextbreakpoint.flink.client.model.JarUploadResponseBody
import com.nextbreakpoint.flink.client.model.JobDetailsInfo
import com.nextbreakpoint.flink.client.model.JobStatus
import com.nextbreakpoint.flink.client.model.Metric
import com.nextbreakpoint.flink.client.model.SavepointInfo
import com.nextbreakpoint.flink.client.model.SavepointTriggerRequestBody
import com.nextbreakpoint.flink.client.model.TaskManagerDetailsInfo
import com.nextbreakpoint.flink.client.model.TaskManagersInfo
import com.nextbreakpoint.flink.client.model.TerminationMode
import com.nextbreakpoint.flink.client.model.TriggerResponse
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger

object FlinkClient {
    private val logger = Logger.getLogger(FlinkClient::class.simpleName)

    private const val TIMEOUT = 10000L

    fun getOverview(address: FlinkAddress): ClusterOverviewWithVersion {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.clusterOverviewWithHttpInfo

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't get cluster overview")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't get cluster overview", e)
        }
    }

    fun listJars(address: FlinkAddress): List<JarFileInfo> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.jarListWithHttpInfo

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't list JARs")
            }

            return response.data.files
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't list JARs", e)
        }
    }

    fun deleteJars(address: FlinkAddress, files: List<JarFileInfo>) {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            files.forEach { file ->
                val response = flinkApi.deleteJarWithHttpInfo(file.id)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't remove JAR")
                }
            }
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't remove JAR", e)
        }
    }

    fun listRunningJobs(address: FlinkAddress): List<String> {
        return listJobs(address, setOf(JobStatus.RUNNING)).keys.toList()
    }

    fun listJobs(address: FlinkAddress, statuses: Set<JobStatus>): Map<String, JobStatus> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.jobIdsWithStatusesOverviewWithHttpInfo

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't get jobs")
            }

            return response.data.jobs.filter {
                jobIdWithStatus -> statuses.isEmpty() || statuses.contains(jobIdWithStatus.status)
            }.map {
                it.id to it.status
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't get jobs", e)
        }
    }

    fun runJar(
        address: FlinkAddress,
        jarFileId: String,
        className: String,
        parallelism: Int,
        savepointPath: String?,
        arguments: List<String>
    ): JarRunResponseBody {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT * 10)

            val response = flinkApi.submitJobFromJarWithHttpInfo(
                jarFileId,
                null,
                false,
                if (savepointPath == "") null else savepointPath,
                arguments.joinToString(separator = " "),
                null,
                className,
                parallelism
            )

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't run JAR")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't run JAR", e)
        }
    }

    fun getCheckpointingStatistics(address: FlinkAddress, jobs: List<String>): Map<String, CheckpointingStatistics> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map { jobId ->
                val response = flinkApi.getCheckpointingStatisticsWithHttpInfo(jobId)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't get checkpointing statistics")
                }

                jobId to response
            }.map {
                it.first to it.second.data
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't get checkpointing statistics", e)
        }
    }

    fun cancelJobs(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String> {
        return createSavepoints(address, jobs, targetPath, true)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()
    }

    fun getJobDetails(address: FlinkAddress, jobId: String): JobDetailsInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobDetailsWithHttpInfo(jobId)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch job details")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch job details", e)
        }
    }

    fun getJobMetrics(address: FlinkAddress, jobId: String, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobMetricsWithHttpInfo(jobId, metricKey)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch job metrics")
            }

            return response.data.toList()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch job metrics", e)
        }
    }

    fun getJobManagerMetrics(address: FlinkAddress, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobManagerMetricsWithHttpInfo(metricKey)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch job manager metrics")
            }

            return response.data.toList()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch job manager metrics", e)
        }
    }

    fun getTaskManagerMetrics(address: FlinkAddress, taskmanagerId: TaskManagerId, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getTaskManagerMetricsWithHttpInfo(taskmanagerId.taskmanagerId, metricKey)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch task manager metrics")
            }

            return response.data.toList()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch task manager metrics", e)
        }
    }

    fun getTaskManagerDetails(address: FlinkAddress, taskmanagerId: TaskManagerId): TaskManagerDetailsInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getTaskManagerDetailsWithHttpInfo(taskmanagerId.taskmanagerId)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch task manager details")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch task manager details", e)
        }
    }

    fun terminateJobs(address: FlinkAddress, jobs: List<String>) {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            jobs.forEach { jobId ->
                val detailsResponse = flinkApi.getJobDetailsWithHttpInfo(jobId)

                if (detailsResponse.statusCode == 404) {
                    return
                }

                if (detailsResponse.statusCode >= 400) {
                    throw FlinkClientException("[$address] Can't get details of job $jobId")
                }

                detailsResponse.data.also { jobDetailsInfo ->
                    if (jobDetailsInfo.state != JobStatus.FINISHED && jobDetailsInfo.state != JobStatus.FAILED && jobDetailsInfo.state != JobStatus.CANCELED) {
                        val response = flinkApi.cancelJobWithHttpInfo(jobId, TerminationMode.CANCEL)

                        if (response.statusCode == 404) {
                            return
                        }

                        if (response.statusCode >= 400) {
                            throw FlinkClientException("[$address] Can't cancel job $jobId")
                        }
                    }
                }
            }
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't terminate jobs", e)
        }
    }

    fun getTaskManagersOverview(address: FlinkAddress): TaskManagersInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.taskManagersWithHttpInfo

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't fetch task managers overview")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't fetch task managers overview", e)
        }
    }

    fun getSavepointRequestsStatus(address: FlinkAddress, requests: Map<String, String>): Map<String, SavepointStatus> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return requests.map { (jobId, requestId) ->
                val response = flinkApi.getSavepointStatusWithHttpInfo(jobId, requestId)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't get savepoint status for job $jobId")
                }

                jobId to response
            }.map {
                it.second.data.let { asynchronousOperationResult ->
                    if (asynchronousOperationResult.status.id != Id.COMPLETED) {
                        logger.info("[$address] Savepoint in progress for job ${it.first}")

                        it.first to SavepointStatus("IN_PROGRESS", null)
                    } else {
                        val operation = asynchronousOperationResult.operation

                        logger.log(Level.FINE, "operation: $operation")

                        if (operation !is SavepointInfo) {
                            throw FlinkClientException("[$address] Can't get savepoint status for job ${it.first}")
                        }

                        val location = operation.location
                        val failureCause = operation.failureCause

                        if (failureCause == null) {
                            logger.info("[$address] Savepoint completed for job ${it.first}")

                            it.first to SavepointStatus("COMPLETED", location)
                        } else {
                            logger.info("[$address] Savepoint failed for job ${it.first} (${failureCause})")

                            it.first to SavepointStatus("FAILED", null)
                        }
                    }
                }
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't get savepoint status", e)
        }
    }

    fun triggerSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String> {
        return createSavepoints(address, jobs, targetPath, false)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()
    }

    private fun createSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?, cancelJob: Boolean): Map<String, TriggerResponse> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map { jobId ->
                val requestBody = SavepointTriggerRequestBody().cancelJob(cancelJob).targetDirectory(targetPath)

                val response = flinkApi.triggerSavepointWithHttpInfo(jobId, requestBody)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't trigger savepoint for job $jobId")
                }

                jobId to response
            }.map {
                it.first to it.second.data
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't trigger savepoint", e)
        }
    }

    fun uploadJar(address: FlinkAddress, file: File): JarUploadResponseBody {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT * 30)

            val response = flinkApi.uploadJarWithHttpInfo(file)

            if (response.statusCode != 200) {
                throw FlinkClientException("[$address] Can't upload JAR")
            }

            return response.data
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't upload JAR", e)
        }
    }

    fun getRescaleRequestsStatus(address: FlinkAddress, requests: Map<String, String>): Map<String, RescaleStatus> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return requests.map { (jobId, requestId) ->
                val response = flinkApi.getRescalingStatusWithHttpInfo(jobId, requestId)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't get rescale status for job $jobId")
                }

                jobId to response
            }.map {
                it.second.data.let { asynchronousOperationResult ->
                    if (asynchronousOperationResult.status.id != Id.COMPLETED) {
                        logger.info("[$address] Rescale in progress for job ${it.first}")

                        it.first to RescaleStatus("IN_PROGRESS")
                    } else {
                        val operation = asynchronousOperationResult.operation

                        logger.log(Level.FINE, "operation: $operation")

                        if (operation !is AsynchronousOperationInfo) {
                            throw FlinkClientException("[$address] Can't get rescale status for job ${it.first}")
                        }

                        val failureCause = operation.failureCause

                        if (failureCause == null) {
                            logger.info("[$address] Rescale completed for job ${it.first}")

                            it.first to RescaleStatus("COMPLETED")
                        } else {
                            logger.info("[$address] Rescale failed for job ${it.first} (${failureCause})")

                            it.first to RescaleStatus("FAILED")
                        }
                    }
                }
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't get rescale status", e)
        }
    }

    fun triggerJobsRescaling(address: FlinkAddress, jobs: List<String>, parallelism: Int): Map<String, String> {
        return triggerJobRescaling(address, jobs, parallelism)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created rescale request ${it.second} for job ${it.first}")
            }.toMap()
    }

    private fun triggerJobRescaling(address: FlinkAddress, jobs: List<String>, parallelism: Int): Map<String, TriggerResponse> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map {
                val response = flinkApi.rescaleJobWithHttpInfo(it, parallelism)

                if (response.statusCode != 200) {
                    throw FlinkClientException("[$address] Can't trigger rescale for job $it")
                }

                it to response
            }.map {
                it.first to it.second.data
            }.toMap()
        } catch (e : FlinkClientException) {
            throw e
        } catch (e : Exception) {
            throw FlinkClientException("[$address] Can't trigger rescale", e)
        }
    }

    private fun createFlinkApiClient(address: FlinkAddress, timeout: Long): FlinkApi {
        val flinkApi = FlinkApi()
        val apiClient = flinkApi.apiClient
        apiClient.basePath = "${address.protocol}://${address.host}:${address.port}"
        apiClient.httpClient = apiClient.httpClient.newBuilder()
            .connectTimeout(timeout, TimeUnit.MILLISECONDS)
            .writeTimeout(timeout, TimeUnit.MILLISECONDS)
            .readTimeout(timeout, TimeUnit.MILLISECONDS)
            .build()
        apiClient.isDebugging = System.getProperty("flink.client.debugging", "false").toBoolean()
        return flinkApi
    }
}