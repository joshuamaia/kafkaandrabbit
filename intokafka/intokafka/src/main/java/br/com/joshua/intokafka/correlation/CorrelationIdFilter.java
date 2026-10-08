package br.com.joshua.intokafka.correlation;

import java.io.IOException;
import java.util.UUID;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Início do fluxo: gera o correlationId de cada requisição REST.
 * <p>
 * O valor é sempre gerado pela aplicação (o cliente não pode informá-lo), colocado no MDC para que
 * todos os logs da requisição o contenham, exposto como atributo da requisição para o controller e
 * devolvido ao cliente no header {@value CorrelationId#HEADER}.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		String correlationId = UUID.randomUUID().toString();
		request.setAttribute(CorrelationId.REQUEST_ATTRIBUTE, correlationId);
		response.setHeader(CorrelationId.HEADER, correlationId);
		try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
			chain.doFilter(request, response);
		}
	}

}
