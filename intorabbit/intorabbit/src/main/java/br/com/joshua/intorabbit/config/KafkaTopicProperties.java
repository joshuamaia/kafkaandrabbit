package br.com.joshua.intorabbit.config;

import java.time.Duration;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Tópico de destino no Kafka, centralizado em {@code app.kafka.*} (application.yml).
 */
@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaTopicProperties(
		@NotBlank String topic,
		@Min(1) int partitions,
		@Min(1) short replicas,
		@NotNull Duration sendTimeout) {
}
