package com.nextbreakpoint.flink.controller.k8s.crd;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true, setterPrefix = "with")
public class V1FlinkJobStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("supervisorStatus")
    private String supervisorStatus;
    @SerializedName("clusterName")
    private String clusterName;
    @SerializedName("clusterUid")
    private String clusterUid;
    @SerializedName("clusterHealth")
    private String clusterHealth;
    @SerializedName("labelSelector")
    private String labelSelector;
    @SerializedName("jobParallelism")
    private Integer jobParallelism;
    @SerializedName("timestamp")
    private OffsetDateTime timestamp;
    @SerializedName("jobId")
    private String jobId;
    @SerializedName("jobStatus")
    private String jobStatus;
    @SerializedName("savepointPath")
    private String savepointPath;
    @SerializedName("savepointJobId")
    private String savepointJobId;
    @SerializedName("savepointTriggerId")
    private String savepointTriggerId;
    @SerializedName("savepointTimestamp")
    private OffsetDateTime savepointTimestamp;
    @SerializedName("savepointRequestTimestamp")
    private OffsetDateTime savepointRequestTimestamp;
    @SerializedName("savepointMode")
    private String savepointMode;
    @SerializedName("restartPolicy")
    private String restartPolicy;
    @SerializedName("bootstrapAttempts")
    private Integer bootstrapAttempts;
    @SerializedName("digest")
    private V1FlinkJobDigest digest;
}
