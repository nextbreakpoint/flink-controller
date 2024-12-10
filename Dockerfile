FROM maven:3.9-eclipse-temurin-21-jammy AS build
ADD . /src
WORKDIR /src
RUN mvn -t toolchains-docker.xml -Pdistribution clean package

#FROM ghcr.io/graalvm/native-image-community:23 as native-image
#COPY --from=build /src/build/libs/* /build/libs/
#COPY --from=build /src/build/.classpath /build/.classpath
#WORKDIR /
#RUN native-image --verbose -J-Xmx4G -cp $(cat /build/.classpath) -H:+UnlockExperimentalVMOptions -H:+StaticExecutableWithDynamicLibC -H:Name=flinkctl -H:Class=com.nextbreakpoint.flink.control.cli.Main

FROM gcr.io/distroless/base
COPY --from=build /src/target/jpackage/flinkctl /flinkctl
EXPOSE 4444 8080
ENTRYPOINT ["/flinkctl/bin/flinkctl"]
