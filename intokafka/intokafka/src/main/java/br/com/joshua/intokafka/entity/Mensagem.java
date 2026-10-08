package br.com.joshua.intokafka.entity;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import br.com.joshua.intokafka.dto.MensagemRequest;

/**
 * Registro de uma etapa do fluxo no banco {@code intokafka}.
 * <p>
 * Cada correlationId gera até dois registros: {@link TipoMensagem#ENVIO} (requisição REST) e
 * {@link TipoMensagem#RETORNO} (mensagem devolvida pelo Kafka). O {@code id} é a chave local; o
 * {@code correlationId} é o identificador global, indexado para rastreamento.
 */
@Entity
@Table(name = "mensagem",
		indexes = @Index(name = "idx_mensagem_correlation_id", columnList = "correlation_id"),
		uniqueConstraints = @UniqueConstraint(name = "uk_mensagem_correlation_id_tipo",
				columnNames = { "correlation_id", "tipo" }))
public class Mensagem {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "correlation_id", nullable = false, updatable = false, length = 36)
	private String correlationId;

	@Column(name = "mensagem", nullable = false, length = MensagemRequest.TAMANHO_MAXIMO)
	private String mensagem;

	@Column(name = "data_cadastro", nullable = false, updatable = false)
	private LocalDateTime dataCadastro;

	@Enumerated(EnumType.STRING)
	@Column(name = "tipo", nullable = false, updatable = false, length = 20)
	private TipoMensagem tipo;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 40)
	private StatusMensagem status;

	@Column(name = "data_atualizacao", nullable = false)
	private LocalDateTime dataAtualizacao;

	protected Mensagem() {
	}

	private Mensagem(String correlationId, String mensagem, LocalDateTime dataCadastro, TipoMensagem tipo,
			StatusMensagem status, LocalDateTime dataAtualizacao) {
		this.correlationId = correlationId;
		this.mensagem = mensagem;
		this.dataCadastro = dataCadastro;
		this.tipo = tipo;
		this.status = status;
		this.dataAtualizacao = dataAtualizacao;
	}

	public static Mensagem envio(String correlationId, String mensagem, LocalDateTime dataCadastro) {
		return new Mensagem(correlationId, mensagem, dataCadastro, TipoMensagem.ENVIO,
				StatusMensagem.AGUARDANDO_PUBLICACAO, dataCadastro);
	}

	public static Mensagem retorno(String correlationId, String mensagem, LocalDateTime dataCadastro,
			LocalDateTime dataRecebimento) {
		return new Mensagem(correlationId, mensagem, dataCadastro, TipoMensagem.RETORNO,
				StatusMensagem.RETORNO_RECEBIDO_KAFKA, dataRecebimento);
	}

	public Long getId() {
		return id;
	}

	public String getCorrelationId() {
		return correlationId;
	}

	public String getMensagem() {
		return mensagem;
	}

	public LocalDateTime getDataCadastro() {
		return dataCadastro;
	}

	public TipoMensagem getTipo() {
		return tipo;
	}

	public StatusMensagem getStatus() {
		return status;
	}

	public LocalDateTime getDataAtualizacao() {
		return dataAtualizacao;
	}

}
