package br.com.joshua.intokafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import br.com.joshua.intokafka.correlation.CorrelationId;
import br.com.joshua.intokafka.dto.MensagemEvent;
import br.com.joshua.intokafka.service.MensagemRetornoService;

/**
 * Consome o retorno publicado pelo intorabbit no Kafka. O correlationId original é mantido: vem do
 * corpo do evento (ou, na falta dele, da chave do registro) e nunca é regenerado.
 */
@Component
public class MensagemKafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(MensagemKafkaConsumer.class);

	private final MensagemRetornoService retornoService;

	private final String applicationName;

	public MensagemKafkaConsumer(MensagemRetornoService retornoService,
			@Value("${spring.application.name}") String applicationName) {
		this.retornoService = retornoService;
		this.applicationName = applicationName;
	}

	@KafkaListener(topics = "${app.kafka.topic}", groupId = "${app.kafka.consumer-group}")
	public void consumir(ConsumerRecord<String, MensagemEvent> record) {
		MensagemEvent event = record.value();
		String correlationId = event != null && event.correlationId() != null ? event.correlationId() : record.key();

		try (MDC.MDCCloseable ignored = MDC.putCloseable(CorrelationId.MDC_KEY, correlationId)) {
			log.info("Mensagem recebida do Kafka [topic={}, partition={}, offset={}]", record.topic(),
					record.partition(), record.offset());
			try {
				retornoService.registrarRetorno(event);
			}
			catch (RuntimeException ex) {
				// O stack trace é registrado pelo error handler (KafkaConfig), que retenta e, por fim, envia à DLT.
				log.error("Falha ao processar mensagem consumida do Kafka [origem={}, destino={}, erro={}]",
						record.topic(), applicationName, ex.getMessage());
				throw ex;
			}
		}
	}

}
