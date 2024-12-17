package com.nextbreakpoint.flink.control.k8s.controller.action

import com.nextbreakpoint.flink.control.common.FlinkOptions
import com.nextbreakpoint.flink.control.k8s.common.FlinkClient
import com.nextbreakpoint.flink.control.k8s.common.KubeClient
import com.nextbreakpoint.flink.control.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.control.testing.KotlinMockito.eq
import com.nextbreakpoint.flink.control.testing.KotlinMockito.given
import com.nextbreakpoint.flink.control.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions

class FlinkClusterCreateTest {
    private val cluster = TestFactory().aFlinkCluster("test", "flink")
    private val flinkOptions = FlinkOptions(hostname = "localhost", portForward = null, useNodePort = false)
    private val flinkClient = mock(FlinkClient::class.java)
    private val kubeClient = mock(KubeClient::class.java)
    private val command = FlinkClusterCreate(flinkOptions, flinkClient, kubeClient)

    @BeforeEach
    fun configure() {
    }

    @Test
    fun `should fail when kubeClient throws exception`() {
        given(kubeClient.createFlinkCluster(eq("flink"), eq(cluster))).thenThrow(RuntimeException::class.java)
        val result = command.execute("flink", "test", cluster)
        verify(kubeClient, times(1)).createFlinkCluster(eq("flink"), eq(cluster))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.ERROR)
        assertThat(result.output).isNull()
    }

    @Test
    fun `should create flink cluster`() {
        val result = command.execute("flink", "test", cluster)
        verify(kubeClient, times(1)).createFlinkCluster(eq("flink"), eq(cluster))
        verifyNoMoreInteractions(kubeClient)
        verifyNoMoreInteractions(flinkClient)
        assertThat(result).isNotNull()
        assertThat(result.status).isEqualTo(ResultStatus.OK)
        assertThat(result.output).isNull()
    }
}