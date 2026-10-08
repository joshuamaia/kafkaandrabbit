package br.com.joshua.intokafka.service;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.test.util.ReflectionTestUtils;

import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.dto.MensagemRequest;
import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.exception.MensagemPublicacaoException;
import br.com.joshua.intokafka.producer.MensagemRabbitProducer;
import br.com.joshua.intokafka.repository.MensagemRepository;

@ExtendWith(MockitoExtension.class)
class MensagemServiceTest {

	private static final String CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000";

	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC);

	@Mock
	private MensagemRepository repository;

	@Mock
	private MensagemRabbitProducer rabbitProducer;

	private MensagemService service;

	@BeforeEach
	void setUp() {
		service = new MensagemService(repository, rabbitProducer, CLOCK);
		when(repository.save(any(Mensagem.class))).thenAnswer(invocation -> {
			Mensagem mensagem = invocation.getArgument(0);
			ReflectionTestUtils.setField(mensagem, "id", 1L);
			return mensagem;
		});
	}

	@Test
	void devePersistirEPublicarNoRabbitComOMesmoCorrelationId() {
		when(repository.findById(1L)).thenReturn(Optional.empty());

		var response = service.registrar(CORRELATION_ID, new MensagemRequest("Teste de integração"));

		ArgumentCaptor<MensagemEvent> event = ArgumentCaptor.forClass(MensagemEvent.class);
		verify(rabbitProducer).publicar(event.capture());
		assertThat(event.getValue().correlationId()).isEqualTo(CORRELATION_ID);
		assertThat(event.getValue().mensagem()).isEqualTo("Teste de integração");
		assertThat(event.getValue().dataCadastro()).isEqualTo(LocalDateTime.now(CLOCK));
		assertThat(response.correlationId()).isEqualTo(CORRELATION_ID);
		verify(repository).atualizarStatusSeAtual(eq(1L), eq(StatusMensagem.AGUARDANDO_PUBLICACAO),
				eq(StatusMensagem.PUBLICADA_RABBITMQ), any());
	}

	@Test
	void deveMarcarFalhaEPropagarErroQuandoRabbitFalhar() {
		doThrow(new MensagemPublicacaoException(CORRELATION_ID, "RabbitMQ", new AmqpException("broker indisponível")))
			.when(rabbitProducer)
			.publicar(any());

		assertThatThrownBy(() -> service.registrar(CORRELATION_ID, new MensagemRequest("Teste")))
			.isInstanceOf(MensagemPublicacaoException.class);

		verify(repository).atualizarStatusSeAtual(eq(1L), eq(StatusMensagem.AGUARDANDO_PUBLICACAO),
				eq(StatusMensagem.FALHA_PUBLICACAO_RABBITMQ), any());
		verify(repository, never()).atualizarStatusSeAtual(any(), any(), eq(StatusMensagem.PUBLICADA_RABBITMQ),
				any());
	}

}
