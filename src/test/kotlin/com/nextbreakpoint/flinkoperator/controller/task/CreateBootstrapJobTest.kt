package com.nextbreakpoint.flinkoperator.context.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.task.CreateBootstrapJob
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.any
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.eq
import com.nextbreakpoint.flinkoperator.testing.KotlinMockito.given
import com.nextbreakpoint.flinkoperator.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class CreateBootstrapJobTest {
    private val clusterId = ClusterId(namespace = "flink", name = "test", uuid = "123")
    private val cluster = TestFactory.aCluster(name = "test", namespace = "flink")
    private val context = mock(TaskContext::class.java)
    private val task = CreateBootstrapJob()

    @BeforeEach
    fun configure() {
        given(context.flinkCluster).thenReturn(cluster)
        given(context.clusterId).thenReturn(clusterId)
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(0)
        Status.setBootstrap(cluster, cluster.spec.bootstrap)
        Status.setJobParallelism(cluster, 1)
        Status.setSavepointPath(cluster, "")
    }

    @Test
    fun `onExecuting should return expected result when job is not defined`() {
        cluster.status.bootstrap = null
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.BOOTSTRAPPING_JOB_TIMEOUT + 1)
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when jar can't be removed`() {
        given(context.removeJar(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).removeJar(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when jar has not been uploaded yet`() {
        given(context.removeJar(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.createBootstrapJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).removeJar(eq(clusterId))
        verify(context, times(1)).createBootstrapJob(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when jar can't be uploaded`() {
        given(context.removeJar(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.createBootstrapJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).removeJar(eq(clusterId))
        verify(context, times(1)).createBootstrapJob(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onExecuting should return expected result when jar has been uploaded`() {
        given(context.removeJar(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.createBootstrapJob(eq(clusterId), any())).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onExecuting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).removeJar(eq(clusterId))
        verify(context, times(1)).createBootstrapJob(eq(clusterId), any())
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when operation times out`() {
        given(context.timeSinceLastUpdateInSeconds()).thenReturn(Timeout.BOOTSTRAPPING_JOB_TIMEOUT + 1)
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.FAILED)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when jar is not ready`() {
        given(context.resources).thenReturn(TestFactory.createResources(clusterId.uuid, cluster))
        given(context.isJarReady(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isJarReady(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when can't get jar status`() {
        given(context.resources).thenReturn(TestFactory.createResources(clusterId.uuid, cluster))
        given(context.isJarReady(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isJarReady(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when jar is ready but job is not started`() {
        given(context.resources).thenReturn(TestFactory.createResources(clusterId.uuid, cluster))
        given(context.isJarReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.isJobStarted(eq(clusterId))).thenReturn(Result(ResultStatus.AWAIT, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isJarReady(eq(clusterId))
        verify(context, times(1)).isJobStarted(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when jar is ready but can't get job status`() {
        given(context.resources).thenReturn(TestFactory.createResources(clusterId.uuid, cluster))
        given(context.isJarReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.isJobStarted(eq(clusterId))).thenReturn(Result(ResultStatus.FAILED, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isJarReady(eq(clusterId))
        verify(context, times(1)).isJobStarted(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onAwaiting should return expected result when jar is ready and job is started`() {
        given(context.resources).thenReturn(TestFactory.createResources(clusterId.uuid, cluster))
        given(context.isJarReady(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        given(context.isJobStarted(eq(clusterId))).thenReturn(Result(ResultStatus.SUCCESS, null))
        val result = task.onAwaiting(context)
        verify(context, atLeastOnce()).clusterId
        verify(context, atLeastOnce()).flinkCluster
        verify(context, atLeastOnce()).timeSinceLastUpdateInSeconds()
        verify(context, times(1)).isJarReady(eq(clusterId))
        verify(context, times(1)).isJobStarted(eq(clusterId))
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.SUCCESS)
        assertThat(result.output).isNotBlank()
    }

    @Test
    fun `onIdle should return expected result`() {
        val result = task.onIdle(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }

    @Test
    fun `onFailed should return expected result`() {
        val result = task.onFailed(context)
        verify(context, atLeastOnce()).flinkCluster
        verifyNoMoreInteractions(context)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.AWAIT)
        assertThat(result.output).isNotNull()
    }
}