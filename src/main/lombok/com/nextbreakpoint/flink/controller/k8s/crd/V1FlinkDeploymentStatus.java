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
public class V1FlinkDeploymentStatus {
    @SerializedName("resourceStatus")
    private String resourceStatus;
    @SerializedName("timestamp")
    private OffsetDateTime timestamp;
    @SerializedName("digest")
    private V1FlinkDeploymentDigest digest;
}
