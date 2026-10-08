package br.com.joshua.intorabbit.correlation;

/**
 * Constantes usadas para propagar o correlationId entre RabbitMQ, Kafka e logs (MDC).
 * <p>
 * Este serviço nunca gera correlationId: ele sempre preserva o valor recebido do intokafka.
 */
public final class CorrelationId {

	/** Chave do MDC utilizada pelo pattern do Logback. */
	public static final String MDC_KEY = "correlationId";

	/** Header AMQP / Kafka que transporta o correlationId. */
	public static final String HEADER = "X-Correlation-Id";

	private CorrelationId() {
	}

}
