package com.nextbreakpoint.flink.control.common

data class ClusterSelector(
    val namespace: String,
    val clusterName: String
) {
    override fun toString(): String {
        return "$namespace:$clusterName"
    }

    fun resourceName() = clusterName
}