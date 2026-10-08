package br.com.joshua.intorabbit.exception;

/**
 * Falha ao publicar um evento no broker de destino.
 */
public class MensagemPublicacaoException extends RuntimeException {

	private final String correlationId;

	public MensagemPublicacaoException(String correlationId, String destino, Throwable cause) {
		super("Falha ao publicar mensagem em " + destino + ": " + cause.getMessage(), cause);
		this.correlationId = correlationId;
	}

	public String getCorrelationId() {
		return correlationId;
	}

}
