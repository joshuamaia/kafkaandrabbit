package br.com.joshua.intokafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * DTO de entrada do endpoint REST. O cliente envia somente o texto da mensagem;
 * id, correlationId e dataCadastro são controlados pela aplicação.
 */
public record MensagemRequest(
		@NotBlank(message = "mensagem é obrigatória")
		@Size(max = MensagemRequest.TAMANHO_MAXIMO, message = "mensagem deve ter no máximo {max} caracteres")
		String mensagem) {

	public static final int TAMANHO_MAXIMO = 4000;

}
