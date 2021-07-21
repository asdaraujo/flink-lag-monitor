/*
 * Copyright (c) 2021, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.examples;

import com.cloudera.examples.data.MessageLag;
import com.cloudera.examples.data.Message;
import com.cloudera.examples.operators.TimestampAndKeyDeserializationSchema;
import com.cloudera.examples.utils.Utils;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

//import static org.apache.flink.table.api.Expressions.*;

public class FlinkLagMonitor {
    static final String SOURCE_PREFIX = "source.";
    static final String TARGET_PREFIX = "target.";
    static final String LAG_PREFIX = "lag.output.";
    static final String STATS_PREFIX = "stats.output.";

    static final String BOOTSTRAP_SERVERS_OPTION = "bootstrap.servers";
    static final String TOPIC_OPTION = "topic";
    static final String PARALLELISM_OPTION = "parallelism";

    static final String AVRO_SCHEMA_FILE_OPTION = "avro.schema.file";
    static final String JSON_OPTION = "use.json";
    static final String CSV_OPTION = "use.csv";
    static final String OFFSET_OPTION = "use.offset";

    static final String PRIMARY_KEY_OPTION = "primary.key";
    static final String JOIN_INTERVAL_MS_OPTION = "join.interval.ms";
    static final String AGGR_WATERMARK_MS_OPTION = "aggr.watermark.ms";
    static final String AGGR_WINDOW_MS_OPTION = "aggr.window.ms";
    static final String PRINT_LAG_OPTION = "print.lag";
    static final String PRINT_STATS_OPTION = "print.stats";

    static final int DEFAULT_PARALLELISM = 1;
    static final int DEFAULT_JOIN_INTERVAL_MS = 10_000;
    static final int DEFAULT_AGGR_WATERMARK_MS = 30_000;
    static final int DEFAULT_AGGR_WINDOW_MS = 60_000;

    static final List<String> NOT_KAFKA_OPTIONS = Arrays.asList(TOPIC_OPTION, JSON_OPTION, AVRO_SCHEMA_FILE_OPTION, PRIMARY_KEY_OPTION);

    static private String sourceTopic = null;
    static private boolean sourceIsJson = false;
    static private Integer sourceCsvField = null;
    static private boolean sourceIsOffset = false;
    static private String sourceSchema = null;
    static private String sourceKey = null;
    static private Properties sourceProps = null;

    static private String targetTopic = null;
    static private boolean targetIsJson = false;
    static private Integer targetCsvField = null;
    static private boolean targetIsOffset = false;
    static private String targetSchema = null;
    static private String targetKey = null;
    static private Properties targetProps = null;

    static private String lagTopic = null;
    static private Properties lagProps = null;

    static private String statsTopic = null;
    static private Properties statsProps = null;

    static private boolean printLag = false;
    static private boolean printStats = false;

    static private int parallelism = DEFAULT_PARALLELISM;
    static private int joinIntervalMs = DEFAULT_JOIN_INTERVAL_MS;
    static private int aggrWatermarkMs = DEFAULT_AGGR_WATERMARK_MS;
    static private int aggrWindowMs = DEFAULT_AGGR_WINDOW_MS;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        parseParameters(params);
        if (params.has("help")) {
            printUsage();
            System.exit(0);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaDeserializationSchema<Message> sourceSerDe = new TimestampAndKeyDeserializationSchema(sourceKey, sourceIsJson ? null : sourceSchema, sourceCsvField, sourceIsOffset);
        System.out.println(sourceProps);
        FlinkKafkaConsumer<Message> sourceConsumer = new FlinkKafkaConsumer<>(sourceTopic, sourceSerDe, sourceProps);
        KeyedStream<Message, Integer> source = env.addSource(sourceConsumer).keyBy(t -> t.hash);

        KafkaDeserializationSchema<Message> targetSerDe = new TimestampAndKeyDeserializationSchema(targetKey, targetIsJson ? null : targetSchema, targetCsvField, targetIsOffset);
        FlinkKafkaConsumer<Message> targetConsumer = new FlinkKafkaConsumer<>(targetTopic, targetSerDe, targetProps);
        KeyedStream<Message, Integer> target = env.addSource(targetConsumer).keyBy(t -> t.hash);

        DataStream<MessageLag> lagStream = source
                .intervalJoin(target)
                .between(Time.milliseconds(0), Time.milliseconds(joinIntervalMs))
                .process (new ProcessJoinFunction<Message, Message, MessageLag>(){
                    @Override
                    public void processElement(Message left, Message right, Context context, Collector<MessageLag> out) throws Exception {
                        out.collect(new MessageLag(left.hash, left.timestamp, right.timestamp));
                    }
                });
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<MessageLag>forBoundedOutOfOrderness(Duration.ofMillis(aggrWatermarkMs))
//                        .withTimestampAssigner((event, timestamp) -> event.timestamp0));

        if (lagTopic != null || printLag) {
            DataStream<String> jsonStream = lagStream.map(MessageLag::toJson);

            if (lagTopic != null)
                jsonStream.addSink(getProducer(lagTopic, lagProps));

            if (printLag)
                jsonStream.print();
        }

//        if (statsTopic != null || printStats) {
//            Table results = tableEnv
//                    .fromDataStream(lagStream,
//                            $("timestamp0").rowtime().as("rowtime"),
//                            $("timestamp0"),
//                            $("timestamp1"))
//                    .addColumns($("timestamp1").minus($("timestamp0")).as("lagMs"))
//                    .window(Tumble
//                            .over(lit(aggrWindowMs).milli())
//                            .on($("rowtime"))
//                            .as("w"))
//                    .groupBy($("w"))
//                    .select(
//                            $("w").start().cast(DataTypes.BIGINT()).as("windowStart"),
//                            $("w").end().cast(DataTypes.BIGINT()).as("windowEnd"),
//                            $("lagMs").min().as("minLagMs"),
//                            $("lagMs").max().as("maxLagMs"),
//                            $("lagMs").avg().as("avgLagMs"),
//                            $("lagMs").stddevPop().as("sdevLagMs"),
//                            $("lagMs").count().as("count")
//                    );
//
//            DataStream<String> statsJsonStream = tableEnv
//                    .toAppendStream(results, MessageLagStats.class)
//                    .map(MessageLagStats::toJson);
//
//            if (statsTopic != null)
//                statsJsonStream.addSink(getProducer(statsTopic, statsProps));
//
//            if (printStats)
//                statsJsonStream.print();
//        }

        env.execute(String.format("Flink Lag Monitor [%s -> %s]", sourceTopic, targetTopic));
    }

    static private void parseParameters(ParameterTool params) throws IOException {
        try {
            sourceTopic = params.getRequired(SOURCE_PREFIX + TOPIC_OPTION);
            sourceIsJson = params.has(SOURCE_PREFIX + JSON_OPTION);
            if (params.has(SOURCE_PREFIX + CSV_OPTION))
                sourceCsvField = params.getInt(SOURCE_PREFIX + CSV_OPTION);
            sourceIsOffset = params.has(SOURCE_PREFIX + OFFSET_OPTION);
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
            sourceProps = Utils.getPrefixedProperties(params.getProperties(), SOURCE_PREFIX, true, NOT_KAFKA_OPTIONS);

            targetTopic = params.getRequired(TARGET_PREFIX + TOPIC_OPTION);
            targetIsJson = params.has(TARGET_PREFIX + JSON_OPTION);
            if (params.has(TARGET_PREFIX + CSV_OPTION))
                targetCsvField = params.getInt(TARGET_PREFIX + CSV_OPTION);
            targetIsOffset = params.has(TARGET_PREFIX + OFFSET_OPTION);
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
            targetProps = Utils.getPrefixedProperties(params.getProperties(), TARGET_PREFIX, true, NOT_KAFKA_OPTIONS);

            lagTopic = params.get(LAG_PREFIX + TOPIC_OPTION);
            lagProps = Utils.getPrefixedProperties(params.getProperties(), LAG_PREFIX, true, NOT_KAFKA_OPTIONS);

            statsTopic = params.get(STATS_PREFIX + TOPIC_OPTION);
            statsProps = Utils.getPrefixedProperties(params.getProperties(), STATS_PREFIX, true, NOT_KAFKA_OPTIONS);

            printLag = params.has(PRINT_LAG_OPTION);
            printStats = params.has(PRINT_STATS_OPTION);

            parallelism = params.getInt(PARALLELISM_OPTION, DEFAULT_PARALLELISM);
            aggrWatermarkMs = params.getInt(AGGR_WATERMARK_MS_OPTION, DEFAULT_AGGR_WATERMARK_MS);
            aggrWindowMs = params.getInt(AGGR_WINDOW_MS_OPTION, DEFAULT_AGGR_WINDOW_MS);
            joinIntervalMs = params.getInt(JOIN_INTERVAL_MS_OPTION, DEFAULT_JOIN_INTERVAL_MS);
        } catch (Exception e) {
            printUsage();
            throw e;
        }
    }

    static private void printUsage() {
        System.out.println("\nArguments: " +
                "--" + SOURCE_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                "--" + SOURCE_PREFIX + TOPIC_OPTION + " <topic> " +
                "--" + SOURCE_PREFIX + PRIMARY_KEY_OPTION + " <key_field> " +
                "[--" + SOURCE_PREFIX + JSON_OPTION + "] " +
                "[--" + SOURCE_PREFIX + AVRO_SCHEMA_FILE_OPTION + " <avsc_file>] " +
                "--" + TARGET_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092> " +
                "--" + TARGET_PREFIX + TOPIC_OPTION + " <topic> " +
                "--" + TARGET_PREFIX + PRIMARY_KEY_OPTION + " <key_field> " +
                "[--" + TARGET_PREFIX + JSON_OPTION + "] " +
                "[--" + TARGET_PREFIX + AVRO_SCHEMA_FILE_OPTION + " <avsc_file>] " +
                "[--" + LAG_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092>] " +
                "[--" + LAG_PREFIX + TOPIC_OPTION + " <topic>] " +
                "[--" + STATS_PREFIX + BOOTSTRAP_SERVERS_OPTION + " <broker1:9092,broker2:9092>] " +
                "[--" + STATS_PREFIX + TOPIC_OPTION + " <topic>] " +
                "[--" + PRINT_LAG_OPTION + "] " +
                "[--" + PRINT_STATS_OPTION + "] " +
                "[--" + AGGR_WINDOW_MS_OPTION + " <int default: " + DEFAULT_AGGR_WINDOW_MS + ">] " +
                "[--" + AGGR_WATERMARK_MS_OPTION + " <int default: " + DEFAULT_AGGR_WATERMARK_MS + ">] " +
                "[--" + JOIN_INTERVAL_MS_OPTION + " <int default: " + DEFAULT_JOIN_INTERVAL_MS + " ms>] " +
                "[--" + PARALLELISM_OPTION + " <int default: " + DEFAULT_PARALLELISM + ">] " +
                ""
        );
    }

    static private FlinkKafkaProducer<String> getProducer(String topic, Properties props) {
        return new FlinkKafkaProducer<>(topic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(topic, element.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }
}
