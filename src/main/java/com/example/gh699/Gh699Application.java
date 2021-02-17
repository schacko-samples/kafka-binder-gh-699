package com.example.gh699;

import java.util.function.Function;
import java.util.logging.Logger;

import brave.kafka.streams.KafkaStreamsTracing;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Gh699Application {

	private static final Logger LOG = Logger.getLogger(Gh699Application.class.getName());

	public static void main(String[] args) {
		SpringApplication.run(Gh699Application.class, args);
	}

	@Bean
	public Function<KStream<String, String>, KStream<String, String>> foo(KafkaStreamsTracing kafkaStreamsTracing) {
		return input -> input.transform(kafkaStreamsTracing.transformer(
				"transformer-1",
				() -> new Transformer<String, String, KeyValue<String, String>>() {
					ProcessorContext context;

					@Override
					public void init(ProcessorContext context) {
						this.context = context;
					}

					@Override
					public KeyValue<String, String> transform(String key, String value) {
						try {
							Thread.sleep(100L);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
						LOG.info("Headers: " + this.context.headers());
						LOG.info("K/V:" + key + "/" + value);
						return KeyValue.pair(key, value);
					}

					@Override
					public void close() {
					}
				}));
	}
}
