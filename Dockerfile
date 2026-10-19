# syntax=docker/dockerfile:1

# ---- build ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# Resolve dependencies first so they are cached between source changes.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw \
 && ./mvnw -B -q dependency:go-offline

COPY src/ src/
# Tests run in CI before the image is built.
RUN ./mvnw -B -q -DskipTests package \
 && ./mvnw -B -q dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib \
 && cp target/ixcache-*.jar target/lib/ixcache.jar

# ---- runtime ----
FROM eclipse-temurin:21-jre

RUN groupadd --system --gid 10940 ixcache \
 && useradd --system --uid 10940 --gid ixcache --home-dir /opt/ixcache --shell /usr/sbin/nologin ixcache \
 && mkdir -p /data /etc/grid-security/certificates \
 && chown ixcache:ixcache /data

COPY --from=build /src/target/lib/ /opt/ixcache/lib/

ENV IXCACHE_CACHE_DIR=/data \
    X509_USER_PROXY=/etc/proxy/x509up \
    X509_CERT_DIR=/etc/grid-security/certificates \
    JDK_JAVA_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+UseZGC -XX:+ExitOnOutOfMemoryError"

USER 10940:10940
WORKDIR /opt/ixcache
VOLUME ["/data"]
EXPOSE 1092

ENTRYPOINT ["java", "-cp", "/opt/ixcache/lib/*", "org.atlasml.ixcache.Server"]
