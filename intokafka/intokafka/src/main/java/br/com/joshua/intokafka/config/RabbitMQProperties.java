package br.com.joshua.intokafka.config;

import java.time.Duration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nomes da topologia RabbitMQ, centralizados em {@code app.rabbitmq.*} (application.yml).
 */
@Validated
@ConfigurationProperties(prefix = "app.rabbitmq")
public record RabbitMQProperties(
		@NotBlank String exchange,
		@NotBlank String queue,
		@NotBlank String routingKey,
		@NotBlank String deadLetterExchange,
		@NotBlank String deadLetterQueue,
		@NotBlank String deadLetterRoutingKey,
		@NotNull Duration confirmTimeout) {
}
