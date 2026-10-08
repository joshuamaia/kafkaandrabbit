package br.com.joshua.intokafka.dto;

import java.time.LocalDateTime;

/**
 * Evento de integração trafegado entre os serviços (RabbitMQ na ida, Kafka na volta).
 * <p>
 * O {@code correlationId} é gerado uma única vez, no início do fluxo, e nunca é regenerado.
 */
public record MensagemEvent(
		String correlationId,
		String mensagem,
		LocalDateTime dataCadastro) {
}
