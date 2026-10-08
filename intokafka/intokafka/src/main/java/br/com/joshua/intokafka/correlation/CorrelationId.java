package br.com.joshua.intokafka.correlation;

/**
 * Constantes usadas para propagar o correlationId entre REST, RabbitMQ, Kafka e logs (MDC).
 */
public final class CorrelationId {

	/** Chave do MDC utilizada pelo pattern do Logback. */
	public static final String MDC_KEY = "correlationId";

	/** Header HTTP / AMQP / Kafka que transporta o correlationId. */
	public static final String HEADER = "X-Correlation-Id";

	/** Atributo da requisição HTTP onde o correlationId gerado fica disponível para o controller. */
	public static final String REQUEST_ATTRIBUTE = "correlationId";

	private CorrelationId() {
	}

}
