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

package com.cloudera.examples.operators;

import com.cloudera.examples.data.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TimestampAndKeyDeserializationSchema implements KafkaDeserializationSchema<Message> {
    private static final Logger LOG = LoggerFactory.getLogger(TimestampAndKeyDeserializationSchema.class);

    private String key = null;
    private String avroSchema = null;
    private transient ObjectMapper mapper = null;
    private transient DatumReader<GenericRecord> datumReader;
    private transient BinaryDecoder decoder = null;
    private transient GenericRecord record = null;

    public TimestampAndKeyDeserializationSchema(String key) {
        this(key, null);
    }

    public TimestampAndKeyDeserializationSchema(String key, String avroSchema) {
        this.key = key;
        this.avroSchema = avroSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (avroSchema == null && mapper == null) // assumes JSON format if a schema is not specified
            mapper = new ObjectMapper();
        if (avroSchema != null) {
            datumReader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(avroSchema));
        }
    }

    @Override
    public boolean isEndOfStream(Message kafkaMessage) {
        return false;
    }

    @Override
    public Message deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        boolean keyFound = false;
        Integer hash = null;
        if (avroSchema == null) {
            Map json = mapper.readValue(consumerRecord.value(), Map.class);
            if (json.containsKey(key)) {
                keyFound = true;
                if (json.get(key) != null)
                    hash = json.get(key).hashCode();
            }
        } else {
            decoder = DecoderFactory.get().binaryDecoder(consumerRecord.value(), decoder);
            record = datumReader.read(record, decoder);
            if (record.hasField(key)) {
                keyFound = true;
                if (record.get(key) != null)
                    hash = record.get(key).hashCode();
            }
        }
        if (!keyFound)
            LOG.warn("Field \"{}\" not found in message", key);
        else if (hash == null)
            LOG.warn("Message has a null key");
        else // hash != null
            return new Message(hash, consumerRecord.timestamp());
        return null;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(new TypeHint<Message>(){});
    }
}
