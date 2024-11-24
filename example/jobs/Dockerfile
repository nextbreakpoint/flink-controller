FROM maven:3.9-eclipse-temurin-11 AS build
COPY demo /demo
WORKDIR /demo
RUN mvn clean package -t toolchains-linux.xml -DincludeFlinkRuntime=false

FROM nextbreakpoint/flinkctl:1.5.0
COPY --from=build /demo/target/demo-1.0.0-shaded.jar /flink-jobs.jar
