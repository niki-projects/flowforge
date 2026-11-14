FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY . .
RUN chmod +x gradlew && ./gradlew --no-daemon bootJar

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 flowforge && useradd --uid 10001 --gid 10001 --no-create-home flowforge
WORKDIR /app
ARG MODULE
COPY --from=build --chown=10001:10001 /workspace/${MODULE}/build/libs/${MODULE}-1.0.0.jar /app/service.jar
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
