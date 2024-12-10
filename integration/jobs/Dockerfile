ARG base_image="integration/flinkctl:latest"
FROM busybox AS download
ARG dummies_version="0.0.5"
RUN wget https://github.com/nextbreakpoint/flink-dummies/releases/download/v${dummies_version}/com.nextbreakpoint.flink.dummies-${dummies_version}.jar -o /com.nextbreakpoint.flink.dummies.jar
FROM ${base_image}
COPY --from=download /com.nextbreakpoint.flink.dummies.jar /
