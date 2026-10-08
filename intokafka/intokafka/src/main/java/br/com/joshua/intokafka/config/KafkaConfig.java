package br.com.joshua.intokafka.config;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.exception.EventoInvalidoException;

/**
 * Tópicos Kafka e tratamento de erros do consumer.
 * <p>
 * Falhas no processamento são retentadas ({@code app.kafka.retry-*}). Esgotadas as tentativas, ou em
 * erros não recuperáveis (JSON inválido, evento sem correlationId), o registro é logado com o
 * correlationId e publicado no Dead Letter Topic, nunca descartado silenciosamente.
 */
@Configuration
public class KafkaConfig {

	private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

	@Bean
	NewTopic mensagensProcessadasTopic(KafkaTopicProperties properties) {
		return TopicBuilder.name(properties.topic())
			.partitions(properties.partitions())
			.replicas(properties.replicas())
			.build();
	}

	@Bean
	NewTopic mensagensProcessadasDeadLetterTopic(KafkaTopicProperties properties) {
		return TopicBuilder.name(properties.deadLetterTopic())
			.partitions(properties.partitions())
			.replicas(properties.replicas())
			.build();
	}

	@Bean
	DefaultErrorHandler kafkaErrorHandler(KafkaProperties kafkaProperties, KafkaTopicProperties properties) {
		// byte[] cobre registros cujo JSON não pôde ser desserializado: o conteúdo original é preservado na DLT.
		DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(
				Map.of(byte[].class, new ByteArraySerializer(), MensagemEvent.class, new JacksonJsonSerializer<>()));
		KafkaTemplate<String, Object> deadLetterTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
				kafkaProperties.buildProducerProperties(), new StringSerializer(), valueSerializer));

		DeadLetterPublishingRecoverer deadLetterRecoverer = new DeadLetterPublishingRecoverer(deadLetterTemplate,
				(record, ex) -> new TopicPartition(properties.deadLetterTopic(), record.partition()));

		// A chave do registro Kafka é o correlationId, então ele está disponível mesmo quando o JSON é inválido.
		ConsumerRecordRecoverer recoverer = (record, ex) -> {
			String correlationId = record.key() != null ? record.key().toString() : null;
			try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
				log.error("Falha ao consumir mensagem do Kafka, enviando para a DLT [origem={}, destino={}, erro={}]",
						record.topic(), properties.deadLetterTopic(),
						NestedExceptionUtils.getMostSpecificCause(ex).getMessage(), ex);
				deadLetterRecoverer.accept(record, ex);
			}
		};

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
				new FixedBackOff(properties.retryInterval().toMillis(), properties.retryMaxAttempts()));
		errorHandler.addNotRetryableExceptions(EventoInvalidoException.class);
		return errorHandler;
	}

}
