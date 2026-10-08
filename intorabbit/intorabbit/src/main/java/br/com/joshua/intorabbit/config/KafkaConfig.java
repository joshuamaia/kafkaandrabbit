package br.com.joshua.intorabbit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declara o tópico de retorno (criado pelo KafkaAdmin caso ainda não exista).
 */
@Configuration
public class KafkaConfig {

	@Bean
	NewTopic mensagensProcessadasTopic(KafkaTopicProperties properties) {
		return TopicBuilder.name(properties.topic())
			.partitions(properties.partitions())
			.replicas(properties.replicas())
			.build();
	}

}
