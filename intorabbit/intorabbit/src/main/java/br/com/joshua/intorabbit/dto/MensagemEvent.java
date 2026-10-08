package br.com.joshua.intorabbit.dto;

import java.time.LocalDateTime;

/**
 * Evento de integração trafegado entre os serviços (RabbitMQ na ida, Kafka na volta).
 * <p>
 * O {@code correlationId} é gerado pelo intokafka no início do fluxo e repassado sem alteração.
 */
public record MensagemEvent(
		String correlationId,
		String mensagem,
		LocalDateTime dataCadastro) {
}
