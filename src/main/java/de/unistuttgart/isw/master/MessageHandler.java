package de.unistuttgart.isw.master;

public interface MessageHandler<V> {
	void handleMessage(V value);
}
