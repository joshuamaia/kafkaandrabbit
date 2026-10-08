package br.com.joshua.intokafka.dto;

import java.time.LocalDateTime;

import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.entity.TipoMensagem;

/**
 * Representação REST de um registro da tabela {@code mensagem}.
 */
public record MensagemResponse(
		Long id,
		String correlationId,
		String mensagem,
		LocalDateTime dataCadastro,
		TipoMensagem tipo,
		StatusMensagem status,
		LocalDateTime dataAtualizacao) {

	public static MensagemResponse from(Mensagem mensagem) {
		return new MensagemResponse(mensagem.getId(), mensagem.getCorrelationId(), mensagem.getMensagem(),
				mensagem.getDataCadastro(), mensagem.getTipo(), mensagem.getStatus(), mensagem.getDataAtualizacao());
	}

}
