package com.nextbreakpoint.flink.controller.k8s.supervisor.core

import com.nextbreakpoint.flink.controller.testing.TestFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class CacheTest {
    private val cache = Cache(namespace = "flink", clusterName = "test")
    private val testFactory = TestFactory()

    @Test
    fun `cache is empty`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return null when looking for non existent cluster`() {
        assertThat(cache.getFlinkCluster("test")).isNull()
    }

    @Test
    fun `should return empty list of clusters initially`() {
        assertThat(cache.getFlinkClusters()).isEmpty()
    }

    @Test
    fun `should return empty list of cluster selectors initially`() {
        assertThat(cache.listClusterNames()).isEmpty()
    }

    @Test
    fun `should update clusters when a cluster changed`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.takeSnapshot()
        assertThat(cache.getFlinkClusters()).isNotEmpty()
    }

    @Test
    fun `should find cluster when a cluster changed`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.takeSnapshot()
        assertThat(cache.getFlinkCluster("test")).isNotNull()
    }

    @Test
    fun `should return non empty list of clusters when a cluster changed`() {
        val cluster1 = testFactory.aFlinkCluster("test", "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = testFactory.aFlinkCluster("test", "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.takeSnapshot()
        val clusters = cache.getFlinkClusters()
        assertThat(clusters).hasSize(1)
        assertThat(clusters).containsExactlyInAnyOrder(cluster2)
    }

    @Test
    fun `should update clusters when a cluster is deleted`() {
        val cluster1 = testFactory.aFlinkCluster("test", "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = testFactory.aFlinkCluster("test", "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.onFlinkClusterDeleted(cluster1)
        cache.takeSnapshot()
        assertThat(cache.getFlinkClusters()).hasSize(0)
    }

    @Test
    fun `should return non empty list of cluster ids when a cluster changed`() {
        val cluster1 = testFactory.aFlinkCluster("test", "flink")
        cluster1.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster1)
        val cluster2 = testFactory.aFlinkCluster("test", "flink")
        cluster2.metadata.uid = "456"
        cache.onFlinkClusterChanged(cluster2)
        cache.takeSnapshot()
        val clusterSelectors = cache.listClusterNames()
        assertThat(clusterSelectors).hasSize(1)
        assertThat(clusterSelectors).containsExactlyInAnyOrder("test")
    }

    @Test
    fun `should throw IllegalStateException when cluster is missing name`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cluster.metadata.name = null
        assertThatThrownBy { cache.onFlinkClusterChanged(cluster) }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `should return empty resources initially`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        cache.takeSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.jobmanagerService).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }

    @Test
    fun `should return resources when resources are changed`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob = testFactory.aFlinkJob("test-test", "flink")
        val job = testFactory.aBootstrapJob(cluster, flinkJob)
        val service = testFactory.aJobManagerService(cluster)
        val jobManagerPod = testFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod = testFactory.aTaskManagerPod(cluster,"1")
        cache.onFlinkJobChanged(flinkJob)
        cache.onJobChanged(job)
        cache.onServiceChanged(service)
        cache.onPodChanged(jobManagerPod)
        cache.onPodChanged(taskManagerPod)
        cache.takeSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEqualTo(setOf(flinkJob))
        assertThat(clusterResources.jobmanagerService).isEqualTo(service)
        assertThat(clusterResources.jobmanagerPods).isEqualTo(setOf(jobManagerPod))
        assertThat(clusterResources.taskmanagerPods).isEqualTo(setOf(taskManagerPod))
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isEqualTo(flinkJob)
        assertThat(jobResources.bootstrapJob).isEqualTo(job)
    }

    @Test
    fun `should update resources when resources are changed`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob1 = testFactory.aFlinkJob("test-test", "flink")
        val job1 = testFactory.aBootstrapJob(cluster, flinkJob1)
        val service1 = testFactory.aJobManagerService(cluster)
        val jobManagerPod1 = testFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod1 = testFactory.aTaskManagerPod(cluster,"1")
        val flinkJob2 = testFactory.aFlinkJob("test-test", "flink")
        val job2 = testFactory.aBootstrapJob(cluster, flinkJob2)
        val service2 = testFactory.aJobManagerService(cluster)
        val jobManagerPod2 = testFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod2 = testFactory.aTaskManagerPod(cluster,"1")
        cache.onFlinkJobChanged(flinkJob1)
        cache.onJobChanged(job1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onFlinkJobChanged(flinkJob2)
        cache.onServiceChanged(service2)
        cache.onJobChanged(job2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.takeSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEqualTo(setOf(flinkJob2))
        assertThat(clusterResources.jobmanagerService).isEqualTo(service2)
        assertThat(clusterResources.jobmanagerPods).isEqualTo(setOf(jobManagerPod2))
        assertThat(clusterResources.taskmanagerPods).isEqualTo(setOf(taskManagerPod2))
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isEqualTo(flinkJob2)
        assertThat(jobResources.bootstrapJob).isEqualTo(job2)
    }

    @Test
    fun `should update resources when resources are deleted`() {
        val cluster = testFactory.aFlinkCluster("test", "flink")
        cluster.metadata.uid = "123"
        cache.onFlinkClusterChanged(cluster)
        val flinkJob1 = testFactory.aFlinkJob("test-test", "flink")
        val job1 = testFactory.aBootstrapJob(cluster, flinkJob1)
        val service1 = testFactory.aJobManagerService(cluster)
        val jobManagerPod1 = testFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod1 = testFactory.aTaskManagerPod(cluster,"1")
        val flinkJob2 = testFactory.aFlinkJob("test-test", "flink")
        val job2 = testFactory.aBootstrapJob(cluster, flinkJob2)
        val service2 = testFactory.aJobManagerService(cluster)
        val jobManagerPod2 = testFactory.aJobManagerPod(cluster,"1")
        val taskManagerPod2 = testFactory.aTaskManagerPod(cluster,"1")
        cache.onFlinkJobChanged(flinkJob1)
        cache.onJobChanged(job1)
        cache.onServiceChanged(service1)
        cache.onPodChanged(jobManagerPod1)
        cache.onPodChanged(taskManagerPod1)
        cache.onFlinkJobChanged(flinkJob2)
        cache.onJobChanged(job2)
        cache.onServiceChanged(service2)
        cache.onPodChanged(jobManagerPod2)
        cache.onPodChanged(taskManagerPod2)
        cache.onFlinkJobDeleted(flinkJob1)
        cache.onJobDeleted(job1)
        cache.onServiceDeleted(service1)
        cache.onPodDeleted(jobManagerPod1)
        cache.onPodDeleted(taskManagerPod1)
        cache.takeSnapshot()
        val clusterResources = cache.getClusterResources()
        assertThat(clusterResources.flinkJobs).isEmpty()
        assertThat(clusterResources.jobmanagerService).isNull()
        assertThat(clusterResources.jobmanagerPods).isEmpty()
        assertThat(clusterResources.taskmanagerPods).isEmpty()
        val jobResources = cache.getJobResources("test")
        assertThat(jobResources.flinkJob).isNull()
        assertThat(jobResources.bootstrapJob).isNull()
    }
}
