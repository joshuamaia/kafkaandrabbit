package br.com.joshua.intorabbit.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import br.com.joshua.intorabbit.config.RabbitMQProperties;
import br.com.joshua.intorabbit.correlation.CorrelationId;
import br.com.joshua.intorabbit.dto.MensagemEvent;
import br.com.joshua.intorabbit.exception.EventoInvalidoException;
import br.com.joshua.intorabbit.service.MensagemService;

/**
 * Consome os eventos publicados pelo intokafka no RabbitMQ. O correlationId recebido é preservado
 * (corpo do evento, ou a propriedade AMQP {@code correlation_id} como fallback) e nunca é regenerado.
 */
@Component
public class MensagemRabbitConsumer {

	private static final Logger log = LoggerFactory.getLogger(MensagemRabbitConsumer.class);

	private final MensagemService mensagemService;

	private final RabbitMQProperties properties;

	private final String applicationName;

	public MensagemRabbitConsumer(MensagemService mensagemService, RabbitMQProperties properties,
			@Value("${spring.application.name}") String applicationName) {
		this.mensagemService = mensagemService;
		this.properties = properties;
		this.applicationName = applicationName;
	}

	@RabbitListener(queues = "${app.rabbitmq.queue}")
	public void consumir(@Payload MensagemEvent event,
			@Header(name = AmqpHeaders.CORRELATION_ID, required = false) String amqpCorrelationId) {
		String correlationId = event.correlationId() != null ? event.correlationId() : amqpCorrelationId;

		try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
			log.info("Mensagem recebida do RabbitMQ [queue={}]", properties.queue());
			try {
				mensagemService.processar(event);
			}
			catch (EventoInvalidoException ex) {
				log.error("Evento inválido recebido do RabbitMQ, enviando para a DLQ [origem={}, destino={}, erro={}]",
						properties.queue(), properties.deadLetterQueue(), ex.getMessage());
				throw new AmqpRejectAndDontRequeueException(ex.getMessage(), ex);
			}
			catch (RuntimeException ex) {
				// O stack trace é registrado pelo container do listener; a mensagem será retentada e, por fim, enviada à DLQ.
				log.error("Falha ao processar mensagem consumida do RabbitMQ [origem={}, destino={}, erro={}]",
						properties.queue(), applicationName, ex.getMessage());
				throw ex;
			}
		}
	}

}
