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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(KeyToValue.class);

    public static final String OVERVIEW_DOC = "Copy fields from record key to record value.";
    public static final String FIELDS_INCLUDE_CONFIG = "fields.include";
    public static final String FIELDS_EXCLUDE_CONFIG = "fields.exclude";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_INCLUDE_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.LOW,
                    "Field names on the record key to copy into the record value.")
            .define(FIELDS_EXCLUDE_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.LOW,
                    "Field names on the record key to not copy into the record value.");

    private static final String PURPOSE = "copying fields from key to value";

    private List<String> includeFields;
    private List<String> excludeFields;

    // Key: key schema, Value: Map of field name -> field schema
    private Cache<Schema, Map<String, Schema>> keySchemaCache;
    // Key: value schema, Value: Map of key schema -> updated value schema
    private Cache<Schema, Map<Schema, Schema>> valueSchemaCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        includeFields = config.getList(FIELDS_INCLUDE_CONFIG);
        excludeFields = config.getList(FIELDS_EXCLUDE_CONFIG);

        keySchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
        valueSchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        Object key = record.key();
        if (key == null) {
            log.debug("key is null, skipping");
            return record;
        }

        Map<String, Object> keysToCopy = record.keySchema() == null ? getKeysToCopySchemaless(key) : getKeysToCopySchema(key);
        if (record.valueSchema() == null) {
            log.debug("apply schemaless");
            return applySchemaless(record, keysToCopy);
        } else {
            log.debug("apply with schema: " + record.valueSchema().fields());
            return applyWithSchema(record, keysToCopy);
        }
    }

    private Map<String, Object> getKeysToCopySchemaless(Object recordKey) {
        final Map<String, Object> key = requireMap(recordKey, PURPOSE);
        Set<String> keysToCopySet = key.keySet();
        if (!includeFields.isEmpty()) {
            keysToCopySet.retainAll(includeFields);
        }
        if (!excludeFields.isEmpty()) {
            excludeFields.forEach(keysToCopySet::remove);
        }

        final Map<String, Object> keysToCopy = new HashMap<>(keysToCopySet.size());
        for (String field : keysToCopySet) {
            keysToCopy.put(field, key.get(field));
        }
        return keysToCopy;
    }

    private Map<String, Object> getKeysToCopySchema(Object recordKey) {
        final Struct key = requireStruct(recordKey, PURPOSE);
        Set<String> keysToCopySet = new HashSet<>();
        for (Field field : key.schema().fields()) {
            if (excludeFields.contains(field.name())) {
                continue;
            }
            if (includeFields.isEmpty() || includeFields.contains(field.name())) {
                keysToCopySet.add(field.name());
            }
        }
        log.debug("keys to copy: " + keysToCopySet);
        final Map<String, Object> keysToCopy = new HashMap<>(keysToCopySet.size());
        final Map<String, Schema> keySchemaMap = new HashMap<>(keysToCopySet.size());
        for (String field : keysToCopySet) {
            keysToCopy.put(field, key.get(field));
            keySchemaMap.put(field, key.schema().field(field).schema());
        }
        keySchemaCache.put(key.schema(), keySchemaMap);
        return keysToCopy;
    }

    private R applySchemaless(R record, Map<String, Object> keysToCopy) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);

        value.putAll(keysToCopy);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, value, record.timestamp());
    }

    private R applyWithSchema(R record, Map<String, Object> keysToCopy) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        log.debug("value schema: " + value.schema().fields());

        Map<Schema, Schema> valueSchemaMap = valueSchemaCache.get(value.schema());
        if (valueSchemaMap == null) {
            valueSchemaMap = new HashMap<>();
        }
        Schema valueSchema = valueSchemaMap.get(record.keySchema());
        if (valueSchema == null) {
            Map<String, Schema> keySchemaMap = keySchemaCache.get(record.keySchema());
            final SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct();
            for (Field field: value.schema().fields()) {
                valueSchemaBuilder.field(field.name(), field.schema());
            }
            for (String field : keysToCopy.keySet()) {
                if (keySchemaMap == null) {
                    valueSchemaBuilder.field(field, Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    valueSchemaBuilder.field(field, keySchemaMap.get(field));
                }
            }

            valueSchema = valueSchemaBuilder.build();
            valueSchemaMap.put(record.keySchema(), valueSchema);
        }
        valueSchemaCache.put(value.schema(), valueSchemaMap);

        Struct newValue = new Struct(valueSchema);
        for (Field field: value.schema().fields()) {
            newValue.put(field.name(), value.get(field.name()));
        }

        log.debug("new value schema: " + valueSchema.fields());
        log.debug("keys to copy: " + keysToCopy.keySet());
        for (Map.Entry<String, Object> entry : keysToCopy.entrySet()) {
            Object valueToCopy = entry.getValue();
            newValue.put(entry.getKey(), record.keySchema() == null ? valueToCopy.toString() : valueToCopy);
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, newValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        keySchemaCache = null;
        valueSchemaCache = null;
    }
}
