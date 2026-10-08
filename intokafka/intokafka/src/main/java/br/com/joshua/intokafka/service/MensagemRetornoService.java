package br.com.joshua.intokafka.service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.entity.TipoMensagem;
import br.com.joshua.intokafka.exception.EventoInvalidoException;
import br.com.joshua.intokafka.repository.MensagemRepository;

/**
 * Fim do fluxo: persiste o retorno recebido via Kafka e marca o envio original como finalizado.
 */
@Service
public class MensagemRetornoService {

	private static final Logger log = LoggerFactory.getLogger(MensagemRetornoService.class);

	private final MensagemRepository repository;

	private final Clock clock;

	public MensagemRetornoService(MensagemRepository repository, Clock clock) {
		this.repository = repository;
		this.clock = clock;
	}

	/**
	 * Idempotente: um retorno já registrado para o mesmo correlationId (reentrega do Kafka) é ignorado.
	 */
	@Transactional
	public void registrarRetorno(MensagemEvent event) {
		validar(event);
		String correlationId = event.correlationId();

		if (repository.existsByCorrelationIdAndTipo(correlationId, TipoMensagem.RETORNO)) {
			log.warn("Retorno já registrado para este correlationId, mensagem duplicada ignorada");
			return;
		}

		LocalDateTime agora = LocalDateTime.now(clock).truncatedTo(ChronoUnit.MICROS);
		Mensagem retorno = repository
			.save(Mensagem.retorno(correlationId, event.mensagem(), event.dataCadastro(), agora));
		log.info("Retorno persistido [banco=intokafka, tabela=mensagem, id={}, tipo={}, status={}]", retorno.getId(),
				retorno.getTipo(), retorno.getStatus());

		int atualizados = repository.atualizarStatus(correlationId, TipoMensagem.ENVIO, StatusMensagem.FINALIZADA,
				agora);
		if (atualizados == 0) {
			log.warn("Registro de ENVIO não encontrado para este correlationId, apenas o RETORNO foi gravado");
		}
		log.info("Fluxo finalizado [REST -> PostgreSQL -> RabbitMQ -> intorabbit -> Kafka -> intokafka]");
	}

	private void validar(MensagemEvent event) {
		if (event == null || !StringUtils.hasText(event.correlationId())) {
			throw new EventoInvalidoException("Evento recebido do Kafka sem correlationId");
		}
		if (event.mensagem() == null || event.dataCadastro() == null) {
			throw new EventoInvalidoException("Evento recebido do Kafka sem mensagem ou dataCadastro");
		}
	}

}
