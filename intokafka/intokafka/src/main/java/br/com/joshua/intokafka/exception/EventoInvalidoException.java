package br.com.joshua.intokafka.exception;

/**
 * Evento de integração sem os dados mínimos (ex.: sem correlationId). Não é retentado.
 */
public class EventoInvalidoException extends RuntimeException {

	public EventoInvalidoException(String message) {
		super(message);
	}

}
