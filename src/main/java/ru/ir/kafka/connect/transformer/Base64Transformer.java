package ru.ir.kafka.connect.transformer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Base64;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Base64Transformer<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(final R record) {
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                toBase64(record.key()),
                record.valueSchema(),
                toBase64(record.value()),
                record.timestamp(),
                record.headers()
        );
    }

    private String toBase64(final Object input) {
        if (!(input instanceof byte[])) {
            throw new IllegalStateException("input must be byte array");
        }

        return new String(Base64.getEncoder().encode((byte[]) input), UTF_8);
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
