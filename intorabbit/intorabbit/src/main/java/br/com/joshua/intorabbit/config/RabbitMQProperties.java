package br.com.joshua.intorabbit.config;

import jakarta.validation.constraints.NotBlank;

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
		@NotBlank String deadLetterRoutingKey) {
}
