package br.com.joshua.intorabbit.service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import br.com.joshua.intorabbit.dto.MensagemEvent;
import br.com.joshua.intorabbit.entity.Mensagem;
import br.com.joshua.intorabbit.entity.StatusMensagem;
import br.com.joshua.intorabbit.exception.EventoInvalidoException;
import br.com.joshua.intorabbit.exception.MensagemPublicacaoException;
import br.com.joshua.intorabbit.producer.MensagemKafkaProducer;
import br.com.joshua.intorabbit.repository.MensagemRepository;

/**
 * Ponte RabbitMQ → Kafka: persiste a mensagem recebida e a republica no Kafka com o mesmo correlationId.
 */
@Service
public class MensagemService {

	private static final Logger log = LoggerFactory.getLogger(MensagemService.class);

	private final MensagemRepository repository;

	private final MensagemKafkaProducer kafkaProducer;

	private final Clock clock;

	public MensagemService(MensagemRepository repository, MensagemKafkaProducer kafkaProducer, Clock clock) {
		this.repository = repository;
		this.kafkaProducer = kafkaProducer;
		this.clock = clock;
	}

	/**
	 * Idempotente: numa reentrega do RabbitMQ o registro existente é reaproveitado e a publicação no Kafka
	 * só é refeita se ainda não tiver sido confirmada.
	 * @throws EventoInvalidoException se o evento não tiver correlationId, mensagem ou dataCadastro
	 * @throws MensagemPublicacaoException se o Kafka não confirmar a publicação
	 */
	public void processar(MensagemEvent event) {
		validar(event);

		Mensagem mensagem = repository.findByCorrelationId(event.correlationId()).map(existente -> {
			log.warn("Mensagem já persistida anteriormente (reentrega do RabbitMQ) [id={}, status={}]",
					existente.getId(), existente.getStatus());
			return existente;
		}).orElseGet(() -> persistir(event));

		if (mensagem.getStatus() == StatusMensagem.PUBLICADA_KAFKA) {
			log.warn("Mensagem já publicada no Kafka anteriormente, nova publicação ignorada");
			return;
		}

		try {
			kafkaProducer.publicar(event);
		}
		catch (MensagemPublicacaoException ex) {
			repository.atualizarStatus(mensagem.getId(), StatusMensagem.FALHA_PUBLICACAO_KAFKA, agora());
			throw ex;
		}
		repository.atualizarStatus(mensagem.getId(), StatusMensagem.PUBLICADA_KAFKA, agora());
	}

	private Mensagem persistir(MensagemEvent event) {
		Mensagem mensagem = repository
			.save(Mensagem.recebida(event.correlationId(), event.mensagem(), event.dataCadastro(), agora()));
		log.info("Mensagem persistida [banco=intorabbit, tabela=mensagem, id={}, status={}]", mensagem.getId(),
				mensagem.getStatus());
		return mensagem;
	}

	private void validar(MensagemEvent event) {
		if (event == null || !StringUtils.hasText(event.correlationId())) {
			throw new EventoInvalidoException("Evento recebido do RabbitMQ sem correlationId");
		}
		if (event.mensagem() == null || event.dataCadastro() == null) {
			throw new EventoInvalidoException("Evento recebido do RabbitMQ sem mensagem ou dataCadastro");
		}
	}

	private LocalDateTime agora() {
		return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MICROS);
	}

}
