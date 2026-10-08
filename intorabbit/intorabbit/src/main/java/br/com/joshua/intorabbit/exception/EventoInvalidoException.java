package br.com.joshua.intorabbit.exception;

/**
 * Evento de integração sem os dados mínimos (ex.: sem correlationId). Não deve ser retentado.
 */
public class EventoInvalidoException extends RuntimeException {

	public EventoInvalidoException(String message) {
		super(message);
	}

}
