package br.com.joshua.intorabbit.producer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import br.com.joshua.intorabbit.config.KafkaTopicProperties;
import br.com.joshua.intorabbit.correlation.CorrelationId;
import br.com.joshua.intorabbit.dto.MensagemEvent;
import br.com.joshua.intorabbit.exception.MensagemPublicacaoException;

/**
 * Publica o {@link MensagemEvent} no Kafka e aguarda o ack do broker.
 * <p>
 * O correlationId vai no corpo JSON, como chave do registro (mantém a ordem por mensagem na mesma
 * partição) e no header {@value CorrelationId#HEADER}.
 */
@Component
public class MensagemKafkaProducer {

	private static final Logger log = LoggerFactory.getLogger(MensagemKafkaProducer.class);

	private final KafkaTemplate<String, MensagemEvent> kafkaTemplate;

	private final KafkaTopicProperties properties;

	private final String applicationName;

	public MensagemKafkaProducer(KafkaTemplate<String, MensagemEvent> kafkaTemplate, KafkaTopicProperties properties,
			@Value("${spring.application.name}") String applicationName) {
		this.kafkaTemplate = kafkaTemplate;
		this.properties = properties;
		this.applicationName = applicationName;
	}

	public void publicar(MensagemEvent event) {
		log.info("Publicando mensagem no Kafka [topic={}]", properties.topic());

		ProducerRecord<String, MensagemEvent> record = new ProducerRecord<>(properties.topic(), event.correlationId(),
				event);
		record.headers().add(CorrelationId.HEADER, event.correlationId().getBytes(StandardCharsets.UTF_8));

		try {
			RecordMetadata metadata = kafkaTemplate.send(record)
				.get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)
				.getRecordMetadata();
			log.info("Mensagem publicada no Kafka [topic={}, partition={}, offset={}]", metadata.topic(),
					metadata.partition(), metadata.offset());
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw falha(event, ex);
		}
		catch (ExecutionException ex) {
			throw falha(event, ex.getCause() != null ? ex.getCause() : ex);
		}
		catch (TimeoutException | KafkaException ex) {
			throw falha(event, ex);
		}
	}

	private MensagemPublicacaoException falha(MensagemEvent event, Throwable cause) {
		log.error("Falha ao publicar mensagem no Kafka [origem={}, destino={}, erro={}]", applicationName,
				properties.topic(), cause.toString(), cause);
		return new MensagemPublicacaoException(event.correlationId(), "Kafka", cause);
	}

}
