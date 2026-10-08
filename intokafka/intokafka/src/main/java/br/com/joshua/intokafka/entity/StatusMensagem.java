package br.com.joshua.intokafka.entity;

public enum StatusMensagem {

	/** ENVIO persistido, ainda não confirmado pelo RabbitMQ. */
	AGUARDANDO_PUBLICACAO,

	/** ENVIO confirmado pelo RabbitMQ (publisher confirm). */
	PUBLICADA_RABBITMQ,

	/** Falha ao publicar o ENVIO no RabbitMQ. */
	FALHA_PUBLICACAO_RABBITMQ,

	/** RETORNO recebido do Kafka. */
	RETORNO_RECEBIDO_KAFKA,

	/** ENVIO cujo retorno já chegou pelo Kafka: fluxo completo. */
	FINALIZADA

}
