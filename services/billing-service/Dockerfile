FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :services:billing-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/billing-service/build/libs/billing-service.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
