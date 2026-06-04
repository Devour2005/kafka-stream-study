package com.kafkastreamstudynew.util;

import com.kafkastreamstudynew.config.KafkaTopicsProperties;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopicUtils {

	private final KafkaTopicsProperties topicsProperties;
	private final KafkaProperties kafkaProperties;

	public void doAdminAction(AdminClientConsumer action) {
		try (Admin client = Admin.create(adminConfig())) {
			action.accept(client);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Map<String, Object> adminConfig() {
		Map<String, Object> properties = kafkaProperties.buildAdminProperties(null);
		properties.putIfAbsent(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		return properties;
	}

	public void createStockTopics() {
		KafkaTopicsProperties.TopicManagement management = topicsProperties.getTopicManagement();
		createTopics(
				management.getPartitions(),
				management.getReplicationFactor(),
				topicsProperties.getAllTopicNames());
	}

	public void recreateStockTopics(int numOfPartitions) {
		this.recreateTopics(numOfPartitions, 1, topicsProperties.getAllTopicNames());
	}

	public void createTopics(int numPartitions, int replicationFactor, Set<String> topics) {
		doAdminAction(admin -> createTopics(admin, numPartitions, replicationFactor, topics));
	}

	public void deleteStockTopics() {
		doAdminAction(admin -> RemoveAllTopics.remove(admin, topicsProperties.getAllTopicNames()));
	}

	public void recreateTopics(int numPartitions, int replicationFactor, Set<String> topics) {
		doAdminAction(admin -> {
			RemoveAllTopics.remove(admin, topics);
			createTopics(admin, numPartitions, replicationFactor, topics);
		});
	}

	public void recreateTopics(int numPartitions, int replicationFactor, String...topics) {
		doAdminAction(admin -> {
			Set<String> topicNames = Set.of(topics);
			RemoveAllTopics.remove(admin, topicNames);
			createTopics(admin, numPartitions, replicationFactor, topicNames);
		});
	}

	private void createTopics(Admin admin, int numPartitions, int replicationFactor, Collection<String> topics)
			throws Exception {

		if (topics.isEmpty()) {
			log.info("No Kafka topics configured for creation");
			return;
		}

		var newTopics = topics.stream()
				.map(topicName -> new NewTopic(topicName, numPartitions, (short) replicationFactor))
				.toList();

		try {
			admin.createTopics(newTopics).all().get();
			log.info("Kafka topics created: {}", topics);
		} catch (Exception ex) {
			if (isTopicExists(ex)) {
				log.info("Kafka topics already exist: {}", topics);
				return;
			}
			throw ex;
		}
	}

	private static boolean isTopicExists(Exception ex) {
		Throwable cause = ex;
		while (cause != null) {
			if (cause instanceof TopicExistsException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}

	@FunctionalInterface
	public interface AdminClientConsumer {
		void accept(Admin client) throws Exception;
	}
}

