package br.com.joshua.intokafka.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.entity.TipoMensagem;
import br.com.joshua.intokafka.exception.EventoInvalidoException;
import br.com.joshua.intokafka.repository.MensagemRepository;

@ExtendWith(MockitoExtension.class)
class MensagemRetornoServiceTest {

	private static final String CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000";

	private static final LocalDateTime DATA_CADASTRO = LocalDateTime.of(2026, 10, 7, 9, 0);

	@Mock
	private MensagemRepository repository;

	private MensagemRetornoService service;

	@BeforeEach
	void setUp() {
		service = new MensagemRetornoService(repository,
				Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC));
	}

	@Test
	void devePersistirRetornoComCorrelationIdOriginalEFinalizarEnvio() {
		when(repository.save(any(Mensagem.class))).thenAnswer(invocation -> invocation.getArgument(0));
		when(repository.atualizarStatus(eq(CORRELATION_ID), eq(TipoMensagem.ENVIO), eq(StatusMensagem.FINALIZADA),
				any()))
			.thenReturn(1);

		service.registrarRetorno(new MensagemEvent(CORRELATION_ID, "Teste", DATA_CADASTRO));

		ArgumentCaptor<Mensagem> retorno = ArgumentCaptor.forClass(Mensagem.class);
		verify(repository).save(retorno.capture());
		assertThat(retorno.getValue().getCorrelationId()).isEqualTo(CORRELATION_ID);
		assertThat(retorno.getValue().getTipo()).isEqualTo(TipoMensagem.RETORNO);
		assertThat(retorno.getValue().getDataCadastro()).isEqualTo(DATA_CADASTRO);
	}

	@Test
	void deveIgnorarRetornoDuplicado() {
		when(repository.existsByCorrelationIdAndTipo(CORRELATION_ID, TipoMensagem.RETORNO)).thenReturn(true);

		service.registrarRetorno(new MensagemEvent(CORRELATION_ID, "Teste", DATA_CADASTRO));

		verify(repository, never()).save(any());
	}

	@Test
	void deveRejeitarEventoSemCorrelationIdSemGerarUmNovo() {
		assertThatThrownBy(() -> service.registrarRetorno(new MensagemEvent(null, "Teste", DATA_CADASTRO)))
			.isInstanceOf(EventoInvalidoException.class);

		verify(repository, never()).save(any());
	}

}
