package br.com.joshua.intorabbit.entity;

public enum StatusMensagem {

	/** Recebida do RabbitMQ e persistida, ainda não publicada no Kafka. */
	RECEBIDA_RABBITMQ,

	/** Publicada no Kafka e confirmada pelo broker. */
	PUBLICADA_KAFKA,

	/** Falha ao publicar no Kafka (a mensagem será reentregue pelo RabbitMQ ou irá para a DLQ). */
	FALHA_PUBLICACAO_KAFKA

}
