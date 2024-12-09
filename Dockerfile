FROM maven:3.9-eclipse-temurin-21-alpine AS build
ADD . /src
WORKDIR /src
RUN mvn clean package
RUN find build/libs -name "*.jar" -print | sed "s/.jar/.jar:/g" | tr -d '\n' | sed "s/:$//g" > /src/build/.classpath

#FROM ghcr.io/graalvm/native-image-community:23 as native-image
#COPY --from=build /src/build/libs/* /build/libs/
#COPY --from=build /src/build/.classpath /build/.classpath
#WORKDIR /
#RUN native-image --verbose -J-Xmx4G -cp $(cat /build/.classpath) -H:+UnlockExperimentalVMOptions -H:+StaticExecutableWithDynamicLibC -H:Name=flinkctl -H:Class=com.nextbreakpoint.flink.control.cli.Main
#
#FROM gcr.io/distroless/base
#COPY --from=native-image flinkctl /flinkctl
#EXPOSE 4444 8080
#ENTRYPOINT ["/flinkctl"]
