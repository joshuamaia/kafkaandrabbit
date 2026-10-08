package br.com.joshua.intokafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import br.com.joshua.intokafka.config.RabbitMQProperties;
import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.exception.MensagemPublicacaoException;

/**
 * Publica o {@link MensagemEvent} no RabbitMQ e aguarda a confirmação do broker (publisher confirm).
 * <p>
 * O correlationId vai no corpo JSON, na propriedade AMQP {@code correlation_id} e no header
 * {@value CorrelationId#HEADER}.
 */
@Component
public class MensagemRabbitProducer {

	private static final Logger log = LoggerFactory.getLogger(MensagemRabbitProducer.class);

	private final RabbitTemplate rabbitTemplate;

	private final RabbitMQProperties properties;

	private final String applicationName;

	public MensagemRabbitProducer(RabbitTemplate rabbitTemplate, RabbitMQProperties properties,
			@Value("${spring.application.name}") String applicationName) {
		this.rabbitTemplate = rabbitTemplate;
		this.properties = properties;
		this.applicationName = applicationName;
	}

	public void publicar(MensagemEvent event) {
		log.info("Publicando mensagem no RabbitMQ [exchange={}, routingKey={}]", properties.exchange(),
				properties.routingKey());
		try {
			rabbitTemplate.invoke(operations -> {
				operations.convertAndSend(properties.exchange(), properties.routingKey(), event, message -> {
					MessageProperties messageProperties = message.getMessageProperties();
					messageProperties.setCorrelationId(event.correlationId());
					messageProperties.setHeader(CorrelationId.HEADER, event.correlationId());
					return message;
				});
				operations.waitForConfirmsOrDie(properties.confirmTimeout().toMillis());
				return null;
			});
		}
		catch (AmqpException ex) {
			log.error("Falha ao publicar mensagem no RabbitMQ [origem={}, destino={}/{}, erro={}]", applicationName,
					properties.exchange(), properties.routingKey(), ex.getMessage(), ex);
			throw new MensagemPublicacaoException(event.correlationId(), "RabbitMQ", ex);
		}
		log.info("Mensagem publicada no RabbitMQ e confirmada pelo broker [exchange={}, routingKey={}]",
				properties.exchange(), properties.routingKey());
	}

}
