# Compiles telemetry-spark-jobs at the commit in .deps/ (see deps.lock) and runs it on Spark 3.5.9 / Temurin 17.
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.12.20 AS build
ARG SBT_REPOSITORIES=""
WORKDIR /src
COPY .deps/telemetry-spark-jobs/ /src/
RUN if [ -n "$SBT_REPOSITORIES" ]; then \
      mkdir -p /root/.sbt && printf '%b\n' "$SBT_REPOSITORIES" > /root/.sbt/repositories && \
      export SBT_OPTS="-Dsbt.override.build.repos=true"; \
    fi && \
    sbt -batch otastatustracker/assembly chargingsessionbuilder/assembly \
      batteryhealthaggregator/assembly diagnosticsdtcrouter/assembly && \
    mkdir -p /jobs && cp jobs/*/target/scala-2.12/*.jar /jobs/

# apache/spark 3.5.9 is built on eclipse-temurin 17 (jre, Ubuntu jammy)
FROM apache/spark:3.5.9-scala2.12-java17-ubuntu
COPY --from=build /jobs/ /opt/jobs/
