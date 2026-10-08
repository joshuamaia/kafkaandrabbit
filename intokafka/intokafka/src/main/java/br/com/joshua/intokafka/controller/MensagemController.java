package br.com.joshua.intokafka.controller;

import java.net.URI;
import java.util.List;

import jakarta.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemRequest;
import br.com.joshua.intokafka.dto.MensagemResponse;
import br.com.joshua.intokafka.service.MensagemService;

@RestController
@RequestMapping("/mensagens")
public class MensagemController {

	private static final Logger log = LoggerFactory.getLogger(MensagemController.class);

	private final MensagemService mensagemService;

	public MensagemController(MensagemService mensagemService) {
		this.mensagemService = mensagemService;
	}

	/**
	 * Inicia o fluxo. O correlationId é gerado pelo {@link br.com.joshua.intokafka.correlation.CorrelationIdFilter}
	 * e devolvido no corpo e no header {@value CorrelationId#HEADER}. Responde 202 porque o restante do fluxo
	 * (RabbitMQ → intorabbit → Kafka) é assíncrono.
	 */
	@PostMapping
	public ResponseEntity<MensagemResponse> criar(
			@RequestAttribute(CorrelationId.REQUEST_ATTRIBUTE) String correlationId,
			@Valid @RequestBody MensagemRequest request) {
		log.info("Requisição REST recebida [POST /mensagens]");
		MensagemResponse response = mensagemService.registrar(correlationId, request);
		URI location = ServletUriComponentsBuilder.fromCurrentRequest()
			.path("/{correlationId}")
			.buildAndExpand(correlationId)
			.toUri();
		return ResponseEntity.accepted().location(location).body(response);
	}

	/**
	 * Consulta os registros (ENVIO e RETORNO) de um correlationId no banco intokafka.
	 */
	@GetMapping("/{correlationId}")
	public ResponseEntity<List<MensagemResponse>> buscar(@PathVariable String correlationId) {
		List<MensagemResponse> mensagens = mensagemService.buscarPorCorrelationId(correlationId);
		return mensagens.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(mensagens);
	}

}
