/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.serdyuk.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * ./bin/kafka-topics --create \
 *           --zookeeper ZOO:2181 \
 *           --replication-factor 1 \
 *           --partitions 1 \
 *           --topic walkersTopic
 *
 * ./bin/kafka-topics --create \
 *           --zookeeper ZOO:2181 \
 *           --replication-factor 1 \
 *           --partitions 1 \
 *           --topic walkTooMuch
 * bin/kafka-console-consumer --topic walkTooMuch --from-beginning \
 *                                       --bootstrap-server BROKER:9092 \
 *                                       --property print.key=true \
 *                                       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 * $ java -cp practice-5.0.0-standalone.jar ru.serdyuk.kafka.WordCountLambdaExample
 *
 * echo -e "Serdyuk Marshirov Baldin Serdyuk Serdyuk Serdyuk Serdyuk Serdyuk Marshirov Marshirov Marshirov" > /tmp/file-input.txt
 *
 * cat /tmp/file-input.txt | ./bin/kafka-console-producer --broker-list BROKER:9092 --topic walkersTopic
 *
 */
public class WordCountLambdaExample {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "blah-wordcount-lambda-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "blah-wordcount-lambda-example-client");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> textLines = builder.stream("walkersTopic");

    final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    final KTable<String, Long> wordCounts = textLines
      .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
      .groupBy((key, word) -> word)
      .count();

    wordCounts.toStream().to("walkTooMuch", Produced.with(stringSerde, longSerde));

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
