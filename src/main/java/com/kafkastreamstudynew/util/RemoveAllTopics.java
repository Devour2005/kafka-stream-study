package com.kafkastreamstudynew.util;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Delete all topics on stopping the application
 */
@Slf4j
public class RemoveAllTopics {

	private static Collection<String> sync(Admin client) throws Exception {
		var topics = client.listTopics()
				.listings()
				.get() // вот тот самый переход
				.stream()
				.map(TopicListing::name)
				.toList();

		log.info("External topics: {}", topics);

		client.deleteTopics(topics).all().get();

		log.info("SUCCESS");

		return topics;
	}

	public static void removeAll(Admin client) throws Exception {
		var topics = sync(client);
		waitUntilDeleted(client, topics);
	}

	public static void remove(Admin client, Collection<String> topics) throws Exception {
		if (topics.isEmpty()) {
			log.info("No Kafka topics configured for deletion");
			return;
		}

		var existingTopics = client.listTopics()
				.names()
				.get()
				.stream()
				.filter(topics::contains)
				.toList();

		if (existingTopics.isEmpty()) {
			log.info("Configured Kafka topics are already absent: {}", topics);
			return;
		}

		log.info("Deleting Kafka topics: {}", existingTopics);
		client.deleteTopics(existingTopics).all().get();
		waitUntilDeleted(client, existingTopics);
		log.info("Kafka topics deleted: {}", existingTopics);
	}

	private static void waitUntilDeleted(Admin client, Collection<String> topics) throws Exception {
		var newTopics = topics.stream().map(t -> new NewTopic(t, 1, (short) 1)).toList();
		var options = new CreateTopicsOptions().validateOnly(true);

		while (true) {
			try {
				client.createTopics(newTopics, options).all().get();
				break;
			} catch (ExecutionException ex) {
				if (ex.getCause() == null || ex.getCause().getClass() != TopicExistsException.class) {
					throw ex;
				}
				Thread.sleep(100);
			}
		}
	}
}
