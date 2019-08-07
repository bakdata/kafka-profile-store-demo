FROM maven:3.6-jdk-11-slim AS build

WORKDIR /app

# Restore maven dependencies in a separate build step
COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src src
RUN mvn package


FROM openjdk:11-jre-slim

WORKDIR /app

COPY --from=build /app/target target

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "target/recommender.jar"]