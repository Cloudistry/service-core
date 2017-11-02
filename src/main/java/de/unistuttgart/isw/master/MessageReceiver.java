package de.unistuttgart.isw.master;

import java.util.ArrayList;
import java.util.List;

public class MessageReceiver<K, V> {
	private final MessageHandler<V> handler;
	private List<String> topics = new ArrayList<>();

	public MessageReceiver(MessageHandler handler, List<String> topics) {
		this.handler = handler;
		this.topics.addAll(topics);
	}

	public List<String> getTopics() {
		return topics;
	}

	void onMessageReceived(V value) {
		try {
			handler.handleMessage(value);
		} catch (Exception e) {
			// TODO
			e.printStackTrace();
		}
	}
}
