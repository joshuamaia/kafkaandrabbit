package br.com.joshua.intokafka.exception;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import br.com.joshua.intokafka.correlation.CorrelationId;

/**
 * Converte exceções em respostas RFC 9457 (Problem Details), sempre incluindo o correlationId da requisição.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

	@ExceptionHandler(MensagemPublicacaoException.class)
	ProblemDetail handlePublicacao(MensagemPublicacaoException ex) {
		// O erro detalhado (origem, destino, causa) já foi logado pelo producer.
		ProblemDetail problem = problem(HttpStatus.SERVICE_UNAVAILABLE,
				"Mensagem persistida, mas não foi possível publicá-la no RabbitMQ. Tente novamente.");
		problem.setTitle("Falha na publicação da mensagem");
		return problem;
	}

	@ExceptionHandler(Exception.class)
	ProblemDetail handleInesperado(Exception ex) {
		log.error("Erro inesperado ao processar requisição REST [erro={}]", ex.getMessage(), ex);
		return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Erro interno ao processar a requisição.");
	}

	@Override
	protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
			HttpHeaders headers, HttpStatusCode status, WebRequest request) {
		Map<String, String> erros = new LinkedHashMap<>();
		ex.getBindingResult().getFieldErrors().forEach(erro -> erros.put(erro.getField(), erro.getDefaultMessage()));
		ProblemDetail problem = problem(status, "Requisição inválida.");
		problem.setProperty("erros", erros);
		return handleExceptionInternal(ex, problem, headers, status, request);
	}

	@Override
	protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
			HttpHeaders headers, HttpStatusCode status, WebRequest request) {
		ProblemDetail problem = problem(status,
				"Corpo da requisição inválido. Envie somente o campo 'mensagem'; id, correlationId e dataCadastro são gerados pela aplicação.");
		return handleExceptionInternal(ex, problem, headers, status, request);
	}

	@Override
	protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
			HttpStatusCode statusCode, WebRequest request) {
		log.warn("Requisição REST rejeitada [status={}, erro={}]", statusCode.value(), ex.getMessage());
		ResponseEntity<Object> response = super.handleExceptionInternal(ex, body, headers, statusCode, request);
		if (response != null && response.getBody() instanceof ProblemDetail problem) {
			problem.setProperty(CorrelationId.MDC_KEY, MDC.get(CorrelationId.MDC_KEY));
		}
		return response;
	}

	private static ProblemDetail problem(HttpStatusCode status, String detail) {
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
		problem.setProperty(CorrelationId.MDC_KEY, MDC.get(CorrelationId.MDC_KEY));
		return problem;
	}

}
