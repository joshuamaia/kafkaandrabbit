# syntax=docker/dockerfile:1

# ---- CAs adicionais (opcional) ----------------------------------------------
# Estágio vazio por padrão. O docker-compose o substitui pelo diretório docker/certs
# (additional_contexts) para builds atrás de proxy/antivírus com inspeção TLS.
FROM scratch AS extra-certs

# ---- Build -------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY --from=extra-certs . /tmp/extra-certs/
RUN find /tmp/extra-certs -type f \( -name '*.crt' -o -name '*.pem' \) | while read -r cert; do \
		keytool -importcert -noprompt -cacerts -storepass changeit -alias "extra-$(basename "$cert")" -file "$cert"; \
	done

COPY pom.xml .
COPY src ./src
# O cache de ~/.m2 acelera rebuilds. Os testes rodam fora do build da imagem (./mvnw test).
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests \
	&& cp target/*.jar app.jar

# ---- Runtime -----------------------------------------------------------------
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S spring && adduser -S spring -G spring
COPY --from=build --chown=spring:spring /workspace/app.jar app.jar
USER spring

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
