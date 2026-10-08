package br.com.joshua.intorabbit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class IntorabbitApplication {

	public static void main(String[] args) {
		SpringApplication.run(IntorabbitApplication.class, args);
	}

}
