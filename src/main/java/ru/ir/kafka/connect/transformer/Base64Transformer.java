package ru.ir.kafka.connect.transformer;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class Base64Transformer<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(final R record) {
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                SchemaBuilder.OPTIONAL_STRING_SCHEMA,
                toBase64(record.key()),
                SchemaBuilder.OPTIONAL_STRING_SCHEMA,
                toBase64(record.value()),
                record.timestamp(),
                record.headers()
        );
    }

    private String toBase64(final Object input) {
        if (input == null) {
            return null;
        }
        if (!(input instanceof byte[])) {
            throw new IllegalStateException("input must be byte array");
        }

        return Base64.encodeBase64String((byte[]) input);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> map) {
    }
}
