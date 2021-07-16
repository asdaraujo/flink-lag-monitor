package com.cloudera.examples;

import com.cloudera.examples.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class FlinkSimpleReplicator {
    static final String SOURCE_PREFIX = "source.";
    static final String TARGET_PREFIX = "target.";
    static final String BOOTSTRAP_SERVERS_OPTION = "bootstrap.servers";
    static final String TOPIC_OPTION = "topic";
    static final String PARALLELISM_OPTION = "parallelism";
    static final int DEFAULT_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() < 4) {
            System.out.println("\nArguments: " +
                    "--" + SOURCE_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                    "--" + SOURCE_PREFIX + TOPIC_OPTION + " <topic> " +
                    "--" + TARGET_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                    "--" + TARGET_PREFIX + TOPIC_OPTION + " <topic> " +
                    "--" + PARALLELISM_OPTION + " <int default: " + DEFAULT_PARALLELISM + ">");
            return;
        }

        String sourceKafka = params.getRequired(SOURCE_PREFIX + BOOTSTRAP_SERVERS_OPTION);
        String sourceTopic = params.getRequired(SOURCE_PREFIX + TOPIC_OPTION);
        Properties sourceProps = Utils.getPrefixedProperties(params.getProperties(), SOURCE_PREFIX, true, Arrays.asList(TOPIC_OPTION));
        String targetKafka = params.getRequired(TARGET_PREFIX + BOOTSTRAP_SERVERS_OPTION);
        String targetTopic = params.getRequired(TARGET_PREFIX + TOPIC_OPTION);
        Properties targetProps = Utils.getPrefixedProperties(params.getProperties(), TARGET_PREFIX, true, Arrays.asList(TOPIC_OPTION));
        int parallelism = params.getInt(PARALLELISM_OPTION, DEFAULT_PARALLELISM);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> sourceConsumer = new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), sourceProps);
        DataStreamSource<String> source = env.addSource(sourceConsumer);

        FlinkKafkaProducer<String> targetProducer = new FlinkKafkaProducer<>(targetTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(targetTopic, element.getBytes());
            }
        }, targetProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        DataStream<String> stream = source
                .map(s -> s.replace("X", "Y"))
                .filter(s -> Math.random() < 0.9);

        stream
                .addSink(targetProducer);
        stream
                .print();

        env.execute("Flink Simple Replicator");
    }
}
