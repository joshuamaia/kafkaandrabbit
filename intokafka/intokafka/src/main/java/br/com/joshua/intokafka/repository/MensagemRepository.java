package br.com.joshua.intokafka.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import br.com.joshua.intokafka.entity.Mensagem;
import br.com.joshua.intokafka.entity.StatusMensagem;
import br.com.joshua.intokafka.entity.TipoMensagem;

public interface MensagemRepository extends JpaRepository<Mensagem, Long> {

	List<Mensagem> findByCorrelationIdOrderByIdAsc(String correlationId);

	boolean existsByCorrelationIdAndTipo(String correlationId, TipoMensagem tipo);

	/**
	 * Atualiza o status apenas se o registro ainda estiver no status esperado. Evita que a confirmação
	 * do RabbitMQ sobrescreva um {@code FINALIZADA} gravado por um retorno do Kafka que chegou antes.
	 */
	@Transactional
	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("""
			update Mensagem m set m.status = :novoStatus, m.dataAtualizacao = :dataAtualizacao
			where m.id = :id and m.status = :statusAtual""")
	int atualizarStatusSeAtual(@Param("id") Long id, @Param("statusAtual") StatusMensagem statusAtual,
			@Param("novoStatus") StatusMensagem novoStatus, @Param("dataAtualizacao") LocalDateTime dataAtualizacao);

	@Transactional
	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("""
			update Mensagem m set m.status = :novoStatus, m.dataAtualizacao = :dataAtualizacao
			where m.correlationId = :correlationId and m.tipo = :tipo""")
	int atualizarStatus(@Param("correlationId") String correlationId, @Param("tipo") TipoMensagem tipo,
			@Param("novoStatus") StatusMensagem novoStatus, @Param("dataAtualizacao") LocalDateTime dataAtualizacao);

}
