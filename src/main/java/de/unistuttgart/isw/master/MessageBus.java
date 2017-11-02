package de.unistuttgart.isw.master;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

import static de.unistuttgart.isw.master.Constants.*;
import static java.util.Objects.requireNonNull;

public class MessageBus<K, V> {
	private Logger logger = LogManager.getLogger(MessageBus.class);

	private List<KafkaConsumer<K, V>> consumers = new ArrayList<>();
	private List<Thread> runningThreads = new ArrayList<>();

	private Map<String, List<MessageReceiver<K, V>>> receivers = new HashMap<>();

	private final KafkaProducer<K, V> producer = buildProducer();
	private final Schema.Parser parser = new Schema.Parser();

	private KafkaProducer<K, V> buildProducer() {
		return new KafkaProducer<>(initProducersConfig());
	}

	private KafkaConsumer<K, V> buildConsumer() {
		return new KafkaConsumer<>(initConsumersConfig());
	}

	private static Properties initKafkaProperties(Properties props) {
		String schemaUrl = requireNonNull(System.getenv(ENV_SCHEMA_URL));
		String brokerConnection = requireNonNull(System.getenv(ENV_BROKER_CONNECTION));

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnection);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

		return props;
	}

	private static Properties initProducersConfig() {
		Properties props = new Properties();

		initKafkaProperties(props);

		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

		return props;
	}

	private static Properties initConsumersConfig() {
		Properties props = new Properties();

		initKafkaProperties(props);

		// TODO not sure how to properly set group for now
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString().replace("-", "_"));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

		return props;
	}

	public MessageSender<K, V> createSender(String portName) {
		return new MessageSender<>(producer, getOutputTopicsForPort(portName));
	}

	private List<String> getOutputTopicsForPort(String portName) {
		return getTopicsForPort(ENV_OUTPUT_PREFIX + portName);
	}

	private List<String> getInputTopicsForPort(String portName) {
		return getTopicsForPort(ENV_INPUT_PREFIX + portName);
	}

	private List<String> getTopicsForPort(String envVariable) {
		String topics = System.getenv(envVariable);
		if (topics == null) {
			logger.info("No topics set for: " + envVariable);
			return Collections.emptyList();
		}
		return Arrays.asList(topics.split(TOPIC_SEPARATOR));
	}

	public void createReceiver(String portName, MessageHandler<V> handler) {
		List<String> topicNames = getInputTopicsForPort(portName);

		if (topicNames.isEmpty()) {
			logger.info("Not creating receiver for port with name: " + portName);
			return;
		}

		KafkaConsumer<K, V> consumer = buildConsumer();
		consumers.add(consumer);
		consumer.subscribe(topicNames);
		MessageReceiver<K, V> receiver = new MessageReceiver<>(handler, topicNames);

		for (String topic : topicNames) {
			receivers.computeIfAbsent(topic, d -> new ArrayList<>()).add(receiver);
		}

		logger.info("New receiver for port with name: " + portName + " on topics: " + topicNames);
	}

	public Schema parseSchema(String schema) {
		return parser.parse(schema);
	}

	public void start() {
		List<Thread> threads = consumers.stream().map(consumer -> new Thread(() -> {
			while (!Thread.interrupted()) {
				ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);

				// TODO what if we want the user to access the whole record?
				for (ConsumerRecord<K, V> record : records) {
					if (!(receivers.containsKey(record.topic()))) {
						continue;
					}

					for (MessageReceiver<K, V> receiver : receivers.get(record.topic())) {
						receiver.onMessageReceived(record.value());
					}
				}
			}
		})).collect(Collectors.toList());

		runningThreads.addAll(threads);
		threads.forEach(Thread::start);
	}

	public void stop() {
		runningThreads.forEach(Thread::interrupt);
	}
}
