FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/detection-engine.jar app.jar
RUN mkdir -p /data
EXPOSE 8084
ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-XX:MaxRAMPercentage=60", "-jar", "/app/app.jar"]
