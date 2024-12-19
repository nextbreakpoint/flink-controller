ARG base_image="nextbreakpoint/flinkctl:latest"
FROM ubuntu:latest AS download
ARG dummies_version="0.0.6"
RUN apt update -y && apt install -y curl
RUN curl https://github.com/nextbreakpoint/flink-dummies/releases/download/v${dummies_version}/com.nextbreakpoint.flink.dummies-${dummies_version}.jar -L -o /com.nextbreakpoint.flink.dummies.jar
FROM ${base_image}
COPY --from=download /com.nextbreakpoint.flink.dummies.jar /
