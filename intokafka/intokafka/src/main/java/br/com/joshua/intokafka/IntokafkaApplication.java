package br.com.joshua.intokafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class IntokafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(IntokafkaApplication.class, args);
	}

}
