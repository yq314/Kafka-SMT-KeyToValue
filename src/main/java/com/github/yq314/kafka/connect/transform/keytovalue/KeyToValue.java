package com.github.yq314.kafka.connect.transform.keytovalue;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELDS_INCLUDE_CONFIG = "fields.include";
    public static final String FIELDS_EXCLUDE_CONFIG = "fields.exclude";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_INCLUDE_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
                    "Field names on the record key to copy into the record value.")
            .define(FIELDS_EXCLUDE_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
                    "Field names on the record key to not copy into the record value.");

    private static final String PURPOSE = "copying fields from key to value";

    private List<String> includeFields;
    private List<String> excludeFields;

    private Cache<Schema, Schema> keyToValueSchemaCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        includeFields = config.getList(FIELDS_INCLUDE_CONFIG);
        excludeFields = config.getList(FIELDS_EXCLUDE_CONFIG);

        keyToValueSchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> key = requireMap(record.key(), PURPOSE);
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        Set<String> keysToCopy = key.keySet();
        if (!includeFields.isEmpty()) {
            keysToCopy.retainAll(includeFields);
        }

        if (!excludeFields.isEmpty()) {
            excludeFields.forEach(keysToCopy::remove);
        }

        for (String field : keysToCopy) {
            value.put(field, key.get(field));
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct key = requireStruct(record.key(), PURPOSE);
        final Struct value = requireStruct(record.value(), PURPOSE);

        Schema valueSchema = keyToValueSchemaCache.get(key.schema());
        if (valueSchema == null) {
            final SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
            for (Field field: value.schema().fields()) {
                valueSchemaBuilder.field(field.name(), field.schema());
            }
            for (Field field : key.schema().fields()) {
                if (excludeFields.contains(field.name())) {
                    continue;
                }
                if (includeFields.isEmpty() || includeFields.contains(field.name())) {
                    valueSchemaBuilder.field(field.name(), field.schema());
                }
            }

            valueSchema = valueSchemaBuilder.build();
            keyToValueSchemaCache.put(key.schema(), valueSchema);
        }

        for (Field field : key.schema().fields()) {
            value.put(field.name(), key.get(field.name()));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), key.schema(), key, valueSchema, value, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        keyToValueSchemaCache = null;
    }
}
