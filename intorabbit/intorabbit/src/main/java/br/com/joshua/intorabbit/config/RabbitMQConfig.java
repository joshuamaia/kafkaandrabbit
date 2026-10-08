package br.com.joshua.intorabbit.config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;

import br.com.joshua.intorabbit.correlation.CorrelationId;
import br.com.joshua.intorabbit.dto.MensagemEvent;
import tools.jackson.databind.json.JsonMapper;

/**
 * Topologia RabbitMQ (exchange, fila, routing key e dead-letter), serialização JSON e destino das
 * mensagens cujas retentativas se esgotaram.
 * <p>
 * A mesma topologia é declarada pelo intokafka. Declarações AMQP são idempotentes, então
 * qualquer um dos serviços pode subir primeiro.
 */
@Configuration
public class RabbitMQConfig {

	/** Identificador lógico do evento no header {@code __TypeId__}, independente do pacote Java de cada serviço. */
	public static final String MENSAGEM_EVENT_TYPE_ID = "MensagemEvent";

	private static final Logger log = LoggerFactory.getLogger(RabbitMQConfig.class);

	@Bean
	DirectExchange mensagensExchange(RabbitMQProperties properties) {
		return ExchangeBuilder.directExchange(properties.exchange()).durable(true).build();
	}

	@Bean
	Queue mensagensQueue(RabbitMQProperties properties) {
		return QueueBuilder.durable(properties.queue())
			.deadLetterExchange(properties.deadLetterExchange())
			.deadLetterRoutingKey(properties.deadLetterRoutingKey())
			.build();
	}

	@Bean
	Binding mensagensBinding(Queue mensagensQueue, DirectExchange mensagensExchange, RabbitMQProperties properties) {
		return BindingBuilder.bind(mensagensQueue).to(mensagensExchange).with(properties.routingKey());
	}

	@Bean
	DirectExchange mensagensDeadLetterExchange(RabbitMQProperties properties) {
		return ExchangeBuilder.directExchange(properties.deadLetterExchange()).durable(true).build();
	}

	@Bean
	Queue mensagensDeadLetterQueue(RabbitMQProperties properties) {
		return QueueBuilder.durable(properties.deadLetterQueue()).build();
	}

	@Bean
	Binding mensagensDeadLetterBinding(Queue mensagensDeadLetterQueue, DirectExchange mensagensDeadLetterExchange,
			RabbitMQProperties properties) {
		return BindingBuilder.bind(mensagensDeadLetterQueue)
			.to(mensagensDeadLetterExchange)
			.with(properties.deadLetterRoutingKey());
	}

	@Bean
	MessageConverter rabbitMessageConverter(JsonMapper jsonMapper) {
		DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
		typeMapper.setIdClassMapping(Map.of(MENSAGEM_EVENT_TYPE_ID, MensagemEvent.class));
		typeMapper.setTrustedPackages(MensagemEvent.class.getPackageName());
		JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter(jsonMapper);
		converter.setJavaTypeMapper(typeMapper);
		return converter;
	}

	/**
	 * Executado quando as retentativas do listener ({@code spring.rabbitmq.listener.simple.retry}) se esgotam:
	 * loga o erro com o correlationId e rejeita a mensagem sem requeue, enviando-a para a DLQ.
	 */
	@Bean
	MessageRecoverer messageRecoverer(RabbitMQProperties properties) {
		RejectAndDontRequeueRecoverer rejectRecoverer = new RejectAndDontRequeueRecoverer();
		return (message, cause) -> {
			String correlationId = message.getMessageProperties().getCorrelationId();
			try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
				log.error("Tentativas esgotadas ao consumir mensagem do RabbitMQ, enviando para a DLQ "
						+ "[origem={}, destino={}, erro={}]", properties.queue(), properties.deadLetterQueue(),
						NestedExceptionUtils.getMostSpecificCause(cause).getMessage());
				rejectRecoverer.recover(message, cause);
			}
		};
	}

}
