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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * $ bin/kafka-topics --create --topic MailTopic \
 *                    --zookeeper ZOO:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic NewMailTopic \
 *                    --zookeeper ZOO:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic OriginalAndNewMailTopic \
 *                    --zookeeper ZOO:2181 --partitions 1 --replication-factor 1
 * $ java -cp practice-5.0.0-standalone.jar ru.serdyuk.kafka.MapFunctionLambdaExample

 * $ bin/kafka-console-producer --broker-list BROKER:9092 --topic MailTopic
 * $ bin/kafka-console-consumer --topic NewMailTopic --from-beginning \
 *                              --bootstrap-server BROKER:9092
 * $ bin/kafka-console-consumer --topic OriginalAndNewMailTopic --from-beginning \
 *                              --bootstrap-server BROKER:9092 --property print.key=true
 * $ bin/kafka-console-consumer --bootstrap-server BROKER:9092 \
 * --topic OriginalAndNewMailTopic    --from-beginning         --formatter kafka.tools.DefaultMessageFormatter \
 * --property print.key=true         \
 * --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 * --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *
 * echo -e "serdyuk@sbrf.ru\nbaldin@sbrf.ru\nmarshirov@sbrf.ru" > /tmp/file-input.txt
 * cat /tmp/file-input.txt | ./bin/kafka-console-producer --broker-list BROKER:9092 --topic MailTopic
 */
public class MapFunctionLambdaExample {

  public static void main(final String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-lambda-example-client");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    final Serde<String> stringSerde = Serdes.String();
    final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<byte[], String> textLines = builder.stream("MailTopic", Consumed.with(byteArraySerde, stringSerde));

    final KStream<byte[], String> newMapValues = textLines.mapValues(v -> v.replace("sbrf", "sbt"));

    newMapValues.to("NewMailTopic");

    final KStream<String, String> originalAndNew = textLines.map((key, value) -> KeyValue.pair(value, value.replace("sbrf", "sbt")));

    originalAndNew.to("OriginalAndNewMailTopic", Produced.with(stringSerde, stringSerde));

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
