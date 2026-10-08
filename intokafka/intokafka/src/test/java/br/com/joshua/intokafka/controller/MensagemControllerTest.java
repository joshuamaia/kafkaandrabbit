package br.com.joshua.intokafka.controller;

import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemRequest;
import br.com.joshua.intokafka.dto.MensagemResponse;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.entity.TipoMensagem;
import br.com.joshua.intokafka.service.MensagemService;

@WebMvcTest(MensagemController.class)
class MensagemControllerTest {

	private static final String UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private MensagemService mensagemService;

	@Test
	void deveGerarCorrelationIdEResponder202() throws Exception {
		when(mensagemService.registrar(anyString(), any(MensagemRequest.class))).thenAnswer(invocation -> {
			String correlationId = invocation.getArgument(0);
			return new MensagemResponse(1L, correlationId, "Teste de integração", LocalDateTime.now(),
					TipoMensagem.ENVIO, StatusMensagem.PUBLICADA_RABBITMQ, LocalDateTime.now());
		});

		MvcResult result = mockMvc
			.perform(post("/mensagens").contentType(MediaType.APPLICATION_JSON)
				.content("{\"mensagem\": \"Teste de integração\"}"))
			.andExpect(status().isAccepted())
			.andExpect(header().string(CorrelationId.HEADER, matchesPattern(UUID_REGEX)))
			.andReturn();

		String correlationId = result.getResponse().getHeader(CorrelationId.HEADER);
		mockMvc.perform(post("/mensagens").contentType(MediaType.APPLICATION_JSON).content("{\"mensagem\": \"x\"}"))
			.andExpect(header().string(CorrelationId.HEADER, not(correlationId)));
		verify(mensagemService).registrar(eq(correlationId), any());
	}

	@Test
	void deveRejeitarCorrelationIdEnviadoPeloCliente() throws Exception {
		mockMvc
			.perform(post("/mensagens").contentType(MediaType.APPLICATION_JSON)
				.content("{\"mensagem\": \"Teste\", \"correlationId\": \"forjado\"}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.correlationId").exists());

		verify(mensagemService, never()).registrar(anyString(), any());
	}

	@Test
	void deveRejeitarMensagemEmBranco() throws Exception {
		mockMvc.perform(post("/mensagens").contentType(MediaType.APPLICATION_JSON).content("{\"mensagem\": \"  \"}"))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.erros.mensagem").exists());
	}

}
