package com.cloudera.examples;

import com.cloudera.examples.data.MessageLag;
import com.cloudera.examples.data.Message;
import com.cloudera.examples.operators.TimestampAndKeyDeserializationSchema;
import com.cloudera.examples.utils.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

public class FlinkLagMonitor {
    static final String SOURCE_PREFIX = "source.";
    static final String TARGET_PREFIX = "target.";
    static final String OUTPUT_PREFIX = "output.";
    static final String BOOTSTRAP_SERVERS_OPTION = "bootstrap.servers";
    static final String TOPIC_OPTION = "topic";
    static final String PARALLELISM_OPTION = "parallelism";
    static final String AVRO_SCHEMA_FILE_OPTION = "avro.schema.file";
    static final String JSON_OPTION = "use.json";
    static final String PRIMARY_KEY_OPTION = "primary.key";
    static final String JOIN_INTERVAL_MS_OPTION = "join.interval.ms";
    static final int DEFAULT_PARALLELISM = 1;
    static final int DEFAULT_INTERVAL_MS_OPTION = 10_000;

    static private String sourceKafka = null;
    static private String sourceTopic = null;
    static private boolean sourceIsJson = false;
    static private String sourceSchema = null;
    static private String sourceKey = null;
    static private Properties sourceProps = null;

    static private String targetKafka = null;
    static private String targetTopic = null;
    static private boolean targetIsJson = false;
    static private String targetSchema = null;
    static private String targetKey = null;
    static private Properties targetProps = null;

    static private String outputKafka = null;
    static private String outputTopic = null;
    static private Properties outputProps = null;

    static private int parallelism = DEFAULT_PARALLELISM;
    static private int joinIntervalMs = DEFAULT_INTERVAL_MS_OPTION;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() < 10) {
            System.out.println("\nArguments: " +
                    "--" + SOURCE_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                    "--" + SOURCE_PREFIX + TOPIC_OPTION + " <topic> " +
                    "[--" + SOURCE_PREFIX + JSON_OPTION + "] " +
                    "--" + SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION + " <avsc_file> " +
                    "--" + SOURCE_PREFIX + PRIMARY_KEY_OPTION + " <key_field> " +
                    "--" + TARGET_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                    "--" + TARGET_PREFIX + TOPIC_OPTION + " <topic> " +
                    "[--" + TARGET_PREFIX + JSON_OPTION + "] " +
                    "--" + TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION + " <avsc_file> " +
                    "--" + TARGET_PREFIX + PRIMARY_KEY_OPTION + " <key_field> " +
                    "--" + OUTPUT_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                    "--" + OUTPUT_PREFIX + TOPIC_OPTION + " <topic> " +
                    "--" + PARALLELISM_OPTION + " <int default: " + DEFAULT_PARALLELISM + ">" +
                    "--" + JOIN_INTERVAL_MS_OPTION + " <int default: " + DEFAULT_INTERVAL_MS_OPTION + " ms>");
            return;
        }
        parseParameters(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaDeserializationSchema<Message> sourceSerDe = new TimestampAndKeyDeserializationSchema(sourceKey, sourceIsJson ? null : sourceSchema);
        FlinkKafkaConsumer<Message> sourceConsumer = new FlinkKafkaConsumer<>(sourceTopic, sourceSerDe, sourceProps);
        KeyedStream<Message, Integer> source = env.addSource(sourceConsumer).keyBy(t -> t.hash);

        KafkaDeserializationSchema<Message> targetSerDe = new TimestampAndKeyDeserializationSchema(targetKey, targetIsJson ? null : targetSchema);
        FlinkKafkaConsumer<Message> targetConsumer = new FlinkKafkaConsumer<>(targetTopic, targetSerDe, targetProps);
        KeyedStream<Message, Integer> target = env.addSource(targetConsumer).keyBy(t -> t.hash);

        FlinkKafkaProducer<String> outputProducer = new FlinkKafkaProducer<>(outputTopic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(outputTopic, element.getBytes());
            }
        }, outputProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        source
                .intervalJoin(target)
                .between(Time.milliseconds(0), Time.milliseconds(joinIntervalMs))
                .process (new ProcessJoinFunction<Message, Message, MessageLag>(){
                    @Override
                    public void processElement(Message left, Message right, Context context, Collector<MessageLag> out) throws Exception {
                        out.collect(new MessageLag(left.hash, left.timestamp, right.timestamp));
                    }
                })
                .map(m -> m.toJson())
                .addSink(outputProducer);

        env.execute("Flink Lag Monitor");
    }

    static private void parseParameters(ParameterTool params) throws IOException {
        sourceKafka = params.getRequired(SOURCE_PREFIX + BOOTSTRAP_SERVERS_OPTION);
        sourceTopic = params.getRequired(SOURCE_PREFIX + TOPIC_OPTION);
        sourceIsJson = params.has(SOURCE_PREFIX + JSON_OPTION);
        sourceSchema = null;
        if (params.has(SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION)) {
            if (sourceIsJson)
                throw new RuntimeException(String.format("The options --%s and --%s and mutually exclusive", SOURCE_PREFIX + JSON_OPTION, SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION));
            String sourceSchemaFile = params.getRequired(SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION);
            sourceSchema = new String(Files.readAllBytes(Paths.get(sourceSchemaFile)));
        }
        if (!sourceIsJson && sourceSchema == null)
            throw new RuntimeException(String.format("Either --%s or --%s must be specified", SOURCE_PREFIX + JSON_OPTION, SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION));
        sourceKey = params.getRequired(SOURCE_PREFIX + PRIMARY_KEY_OPTION);
        sourceProps = Utils.getPrefixedProperties(params.getProperties(), SOURCE_PREFIX, true, Arrays.asList(BOOTSTRAP_SERVERS_OPTION, TOPIC_OPTION, AVRO_SCHEMA_FILE_OPTION, PRIMARY_KEY_OPTION));

        targetKafka = params.getRequired(TARGET_PREFIX + BOOTSTRAP_SERVERS_OPTION);
        targetTopic = params.getRequired(TARGET_PREFIX + TOPIC_OPTION);
        targetIsJson = params.has(TARGET_PREFIX + JSON_OPTION);
        targetSchema = null;
        if (params.has(TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION)) {
            if (targetIsJson)
                throw new RuntimeException(String.format("The options --%s and --%s and mutually exclusive", TARGET_PREFIX + JSON_OPTION, TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION));
            String targetSchemaFile = params.getRequired(TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION);
            targetSchema = new String(Files.readAllBytes(Paths.get(targetSchemaFile)));
        }
        if (!targetIsJson && targetSchema == null)
            throw new RuntimeException(String.format("Either --%s or --%s must be specified", TARGET_PREFIX + JSON_OPTION, TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION));
        targetKey = params.getRequired(TARGET_PREFIX + PRIMARY_KEY_OPTION);
        targetProps = Utils.getPrefixedProperties(params.getProperties(), TARGET_PREFIX, true, Arrays.asList(BOOTSTRAP_SERVERS_OPTION, TOPIC_OPTION, AVRO_SCHEMA_FILE_OPTION, PRIMARY_KEY_OPTION));

        outputKafka = params.getRequired(OUTPUT_PREFIX + BOOTSTRAP_SERVERS_OPTION);
        outputTopic = params.getRequired(OUTPUT_PREFIX + TOPIC_OPTION);
        outputProps = Utils.getPrefixedProperties(params.getProperties(), OUTPUT_PREFIX, true, Arrays.asList(BOOTSTRAP_SERVERS_OPTION, TOPIC_OPTION, AVRO_SCHEMA_FILE_OPTION, PRIMARY_KEY_OPTION));

        parallelism = params.getInt(PARALLELISM_OPTION, DEFAULT_PARALLELISM);
        joinIntervalMs = params.getInt(JOIN_INTERVAL_MS_OPTION, DEFAULT_INTERVAL_MS_OPTION);
    }
}
