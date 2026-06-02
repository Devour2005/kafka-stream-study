package com.kafkastreamstudynew;

import com.kafkastreamstudynew.config.KafkaTopicsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(KafkaTopicsProperties.class)
public class KafkaStreamStudyNewApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamStudyNewApplication.class, args);
	}

}
