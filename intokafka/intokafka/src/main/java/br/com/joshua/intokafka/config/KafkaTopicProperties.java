package br.com.joshua.intokafka.config;

import java.time.Duration;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Tópicos, consumer group e política de retentativa do Kafka, centralizados em {@code app.kafka.*}.
 */
@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaTopicProperties(
		@NotBlank String topic,
		@NotBlank String consumerGroup,
		@NotBlank String deadLetterTopic,
		@Min(1) int partitions,
		@Min(1) short replicas,
		@NotNull Duration retryInterval,
		@Min(0) long retryMaxAttempts) {
}
