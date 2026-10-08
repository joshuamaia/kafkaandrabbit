package br.com.joshua.intokafka.config;

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
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemEvent;
import tools.jackson.databind.json.JsonMapper;

/**
 * Topologia RabbitMQ (exchange, fila, routing key e dead-letter) e serialização JSON.
 * <p>
 * A mesma topologia é declarada pelo intorabbit. Declarações AMQP são idempotentes, então
 * qualquer um dos serviços pode subir primeiro sem que as mensagens fiquem sem rota.
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
	 * RabbitTemplate com {@code mandatory=true}: mensagens sem rota retornam ao produtor e são logadas como erro.
	 */
	@Bean
	RabbitTemplate rabbitTemplate(RabbitTemplateConfigurer configurer, ConnectionFactory connectionFactory,
			@Value("${spring.application.name}") String applicationName) {
		RabbitTemplate template = new RabbitTemplate();
		configurer.configure(template, connectionFactory);
		template.setMandatory(true);
		template.setReturnsCallback(returned -> {
			String correlationId = returned.getMessage().getMessageProperties().getCorrelationId();
			try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
				log.error("Mensagem retornada pelo RabbitMQ por não ter rota [origem={}, destino={}/{}, erro={} {}]",
						applicationName, returned.getExchange(), returned.getRoutingKey(), returned.getReplyCode(),
						returned.getReplyText());
			}
		});
		return template;
	}

}
