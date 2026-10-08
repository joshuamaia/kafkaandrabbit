package br.com.joshua.intokafka.service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.dto.MensagemRequest;
import br.com.joshua.intokafka.dto.MensagemResponse;
import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.exception.MensagemPublicacaoException;
import br.com.joshua.intokafka.producer.MensagemRabbitProducer;
import br.com.joshua.intokafka.repository.MensagemRepository;

/**
 * Início do fluxo: persiste a mensagem recebida via REST e a publica no RabbitMQ.
 */
@Service
public class MensagemService {

	private static final Logger log = LoggerFactory.getLogger(MensagemService.class);

	private final MensagemRepository repository;

	private final MensagemRabbitProducer rabbitProducer;

	private final Clock clock;

	public MensagemService(MensagemRepository repository, MensagemRabbitProducer rabbitProducer, Clock clock) {
		this.repository = repository;
		this.rabbitProducer = rabbitProducer;
		this.clock = clock;
	}

	/**
	 * @param correlationId identificador gerado no início da requisição REST
	 * @throws MensagemPublicacaoException se o RabbitMQ não confirmar a publicação (o registro fica com
	 * status {@link StatusMensagem#FALHA_PUBLICACAO_RABBITMQ})
	 */
	public MensagemResponse registrar(String correlationId, MensagemRequest request) {
		Mensagem mensagem = repository.save(Mensagem.envio(correlationId, request.mensagem(), agora()));
		log.info("Mensagem persistida [banco=intokafka, tabela=mensagem, id={}, tipo={}, status={}]",
				mensagem.getId(), mensagem.getTipo(), mensagem.getStatus());

		MensagemEvent event = new MensagemEvent(mensagem.getCorrelationId(), mensagem.getMensagem(),
				mensagem.getDataCadastro());
		try {
			rabbitProducer.publicar(event);
		}
		catch (MensagemPublicacaoException ex) {
			repository.atualizarStatusSeAtual(mensagem.getId(), StatusMensagem.AGUARDANDO_PUBLICACAO,
					StatusMensagem.FALHA_PUBLICACAO_RABBITMQ, agora());
			throw ex;
		}

		repository.atualizarStatusSeAtual(mensagem.getId(), StatusMensagem.AGUARDANDO_PUBLICACAO,
				StatusMensagem.PUBLICADA_RABBITMQ, agora());
		return MensagemResponse.from(repository.findById(mensagem.getId()).orElse(mensagem));
	}

	@Transactional(readOnly = true)
	public List<MensagemResponse> buscarPorCorrelationId(String correlationId) {
		return repository.findByCorrelationIdOrderByIdAsc(correlationId).stream().map(MensagemResponse::from).toList();
	}

	/** Precisão de microssegundos, a mesma do timestamp do PostgreSQL: evento e banco carregam o mesmo valor. */
	private LocalDateTime agora() {
		return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MICROS);
	}

}
