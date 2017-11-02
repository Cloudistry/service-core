package de.unistuttgart.isw.master;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

public class MessageSender<K, V> {
	private final KafkaProducer<K, V> producer;
	private final List<String> topicNames = new ArrayList<>();

	MessageSender(KafkaProducer<K, V> producer, List<String> topicNames) {
		this.producer = producer;
		this.topicNames.addAll(topicNames);
	}

	public List<String> getTopics() {
		return topicNames;
	}

	public void send(K key, V value) {
		topicNames.forEach(topic -> producer.send(new ProducerRecord<>(topic, key, value)));
	}
}
