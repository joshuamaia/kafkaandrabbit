package br.com.joshua.intorabbit.entity;

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

/**
 * Mensagem recebida do RabbitMQ, persistida no banco {@code intorabbit}.
 * <p>
 * O {@code id} é a chave local; o {@code correlationId} é o identificador global do fluxo, com índice
 * único (cada mensagem é registrada uma única vez, mesmo com reentregas do RabbitMQ).
 */
@Entity
@Table(name = "mensagem",
		indexes = @Index(name = "idx_mensagem_correlation_id", columnList = "correlation_id", unique = true))
public class Mensagem {

	/** Mesmo limite aceito pelo endpoint REST do intokafka. */
	public static final int TAMANHO_MAXIMO_MENSAGEM = 4000;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "correlation_id", nullable = false, updatable = false, length = 36)
	private String correlationId;

	@Column(name = "mensagem", nullable = false, length = TAMANHO_MAXIMO_MENSAGEM)
	private String mensagem;

	@Column(name = "data_cadastro", nullable = false, updatable = false)
	private LocalDateTime dataCadastro;

	@Column(name = "data_recebimento", nullable = false, updatable = false)
	private LocalDateTime dataRecebimento;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 40)
	private StatusMensagem status;

	@Column(name = "data_atualizacao", nullable = false)
	private LocalDateTime dataAtualizacao;

	protected Mensagem() {
	}

	private Mensagem(String correlationId, String mensagem, LocalDateTime dataCadastro,
			LocalDateTime dataRecebimento) {
		this.correlationId = correlationId;
		this.mensagem = mensagem;
		this.dataCadastro = dataCadastro;
		this.dataRecebimento = dataRecebimento;
		this.status = StatusMensagem.RECEBIDA_RABBITMQ;
		this.dataAtualizacao = dataRecebimento;
	}

	/**
	 * @param dataCadastro data original gerada pelo intokafka (preservada)
	 * @param dataRecebimento momento em que o intorabbit recebeu a mensagem
	 */
	public static Mensagem recebida(String correlationId, String mensagem, LocalDateTime dataCadastro,
			LocalDateTime dataRecebimento) {
		return new Mensagem(correlationId, mensagem, dataCadastro, dataRecebimento);
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

	public LocalDateTime getDataRecebimento() {
		return dataRecebimento;
	}

	public StatusMensagem getStatus() {
		return status;
	}

	public LocalDateTime getDataAtualizacao() {
		return dataAtualizacao;
	}

}
