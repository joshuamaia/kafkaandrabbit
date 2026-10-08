package br.com.joshua.intorabbit.repository;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import br.com.joshua.intorabbit.entity.Mensagem;
import br.com.joshua.intorabbit.entity.StatusMensagem;

public interface MensagemRepository extends JpaRepository<Mensagem, Long> {

	Optional<Mensagem> findByCorrelationId(String correlationId);

	@Transactional
	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query("update Mensagem m set m.status = :status, m.dataAtualizacao = :dataAtualizacao where m.id = :id")
	int atualizarStatus(@Param("id") Long id, @Param("status") StatusMensagem status,
			@Param("dataAtualizacao") LocalDateTime dataAtualizacao);

}
