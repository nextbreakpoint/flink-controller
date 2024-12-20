package com.nextbreakpoint.flink.controller.k8s.common

import com.nextbreakpoint.flink.controller.common.RestartPolicy
import com.nextbreakpoint.flink.controller.common.SavepointMode
import com.nextbreakpoint.flink.controller.common.SavepointRequest
import com.nextbreakpoint.flink.controller.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FlinkJobStatusTest {
    private val flinkJob = TestFactory().aFlinkJob("test-test", "flink")

    @Test
    fun `should store savepoint path and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        FlinkJobStatus.setSavepointPath(flinkJob, "/tmp/xxx")
        assertThat(FlinkJobStatus.getSavepointPath(flinkJob)).isEqualTo("/tmp/xxx")
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should not store savepoint path and update timestamp if path is blank`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        FlinkJobStatus.setSavepointPath(flinkJob, "")
        assertThat(FlinkJobStatus.getSavepointPath(flinkJob)).isNull()
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
    }

    @Test
    fun `should store savepoint request and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointRequest(flinkJob)).isNull()
        FlinkJobStatus.setSavepointRequest(flinkJob, SavepointRequest("000", "XXX"))
        assertThat(FlinkJobStatus.getSavepointRequest(flinkJob)).isEqualTo(SavepointRequest("000", "XXX"))
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should reset savepoint request and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        FlinkJobStatus.resetSavepointRequest(flinkJob)
        assertThat(FlinkJobStatus.getSavepointRequest(flinkJob)).isNull()
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
        assertThat(FlinkJobStatus.getSavepointRequestTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should store flink job digest and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getBootstrapDigest(flinkJob)).isNull()
        FlinkJobStatus.setBootstrapDigest(flinkJob, "XXX")
        assertThat(FlinkJobStatus.getBootstrapDigest(flinkJob)).isEqualTo("XXX")
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should store job parallelism and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getJobParallelism(flinkJob)).isEqualTo(0)
        FlinkJobStatus.setJobParallelism(flinkJob, 4)
        assertThat(FlinkJobStatus.getJobParallelism(flinkJob)).isEqualTo(4)
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should store savepoint mode and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getSavepointMode(flinkJob)).isEqualTo(SavepointMode.Automatic)
        FlinkJobStatus.setSavepointMode(flinkJob, SavepointMode.Manual)
        assertThat(FlinkJobStatus.getSavepointMode(flinkJob)).isEqualTo(SavepointMode.Manual)
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }

    @Test
    fun `should store job restart policy and update timestamp`() {
        val timestamp = Time.toDateTime(System.currentTimeMillis())
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isEqualTo(Time.toDateTime(0))
        assertThat(FlinkJobStatus.getRestartPolicy(flinkJob)).isEqualTo(RestartPolicy.Always)
        FlinkJobStatus.setRestartPolicy(flinkJob, RestartPolicy.Never)
        assertThat(FlinkJobStatus.getRestartPolicy(flinkJob)).isEqualTo(RestartPolicy.Never)
        assertThat(FlinkJobStatus.getStatusTimestamp(flinkJob)).isAfterOrEqualTo(timestamp)
    }
}
