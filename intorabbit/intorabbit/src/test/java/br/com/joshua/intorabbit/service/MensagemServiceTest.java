package br.com.joshua.intorabbit.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.test.util.ReflectionTestUtils;

import br.com.joshua.intorabbit.dto.MensagemEvent;
import br.com.joshua.intorabbit.entity.Mensagem;
import br.com.joshua.intorabbit.entity.StatusMensagem;
import br.com.joshua.intorabbit.exception.EventoInvalidoException;
import br.com.joshua.intorabbit.exception.MensagemPublicacaoException;
import br.com.joshua.intorabbit.producer.MensagemKafkaProducer;
import br.com.joshua.intorabbit.repository.MensagemRepository;

@ExtendWith(MockitoExtension.class)
class MensagemServiceTest {

	private static final String CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000";

	private static final MensagemEvent EVENT = new MensagemEvent(CORRELATION_ID, "Teste de integração",
			LocalDateTime.of(2026, 10, 7, 9, 0));

	@Mock
	private MensagemRepository repository;

	@Mock
	private MensagemKafkaProducer kafkaProducer;

	private MensagemService service;

	@BeforeEach
	void setUp() {
		service = new MensagemService(repository, kafkaProducer,
				Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC));
	}

	@Test
	void devePersistirERepublicarNoKafkaOMesmoEvento() {
		when(repository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.empty());
		when(repository.save(any(Mensagem.class))).thenAnswer(invocation -> comId(invocation.getArgument(0)));

		service.processar(EVENT);

		verify(kafkaProducer).publicar(EVENT);
		verify(repository).atualizarStatus(eq(1L), eq(StatusMensagem.PUBLICADA_KAFKA), any());
	}

	@Test
	void naoDeveRepublicarMensagemJaPublicada() {
		Mensagem existente = comId(Mensagem.recebida(CORRELATION_ID, "Teste", EVENT.dataCadastro(),
				LocalDateTime.now()));
		ReflectionTestUtils.setField(existente, "status", StatusMensagem.PUBLICADA_KAFKA);
		when(repository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.of(existente));

		service.processar(EVENT);

		verify(kafkaProducer, never()).publicar(any());
		verify(repository, never()).save(any());
	}

	@Test
	void deveMarcarFalhaEPropagarErroQuandoKafkaFalhar() {
		when(repository.findByCorrelationId(CORRELATION_ID)).thenReturn(Optional.empty());
		when(repository.save(any(Mensagem.class))).thenAnswer(invocation -> comId(invocation.getArgument(0)));
		doThrow(new MensagemPublicacaoException(CORRELATION_ID, "Kafka", new KafkaException("timeout")))
			.when(kafkaProducer)
			.publicar(any());

		assertThatThrownBy(() -> service.processar(EVENT)).isInstanceOf(MensagemPublicacaoException.class);

		verify(repository).atualizarStatus(eq(1L), eq(StatusMensagem.FALHA_PUBLICACAO_KAFKA), any());
	}

	@Test
	void deveRejeitarEventoSemCorrelationId() {
		assertThatThrownBy(() -> service.processar(new MensagemEvent(" ", "Teste", EVENT.dataCadastro())))
			.isInstanceOf(EventoInvalidoException.class);

		verify(kafkaProducer, never()).publicar(any());
	}

	private static Mensagem comId(Mensagem mensagem) {
		ReflectionTestUtils.setField(mensagem, "id", 1L);
		return mensagem;
	}

}
