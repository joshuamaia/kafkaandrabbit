package br.com.joshua.intokafka.entity;

/**
 * Etapa do fluxo representada por um registro da tabela {@code mensagem}.
 */
public enum TipoMensagem {

	/** Mensagem recebida via REST e enviada ao RabbitMQ. */
	ENVIO,

	/** Retorno da mesma mensagem recebido via Kafka. */
	RETORNO

}
