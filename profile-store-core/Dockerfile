FROM maven:3.6-jdk-11-slim AS build

WORKDIR /app

# Restore maven dependencies in a separate build step
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src src
RUN mvn package -DskipTests


FROM openjdk:11-jre-slim

WORKDIR /app

COPY --from=build /app/target target
COPY --from=build /root/.m2 /root/.m2

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "target/core.jar"]