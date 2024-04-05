package com.github.yq314.kafka.connect.transform.keytovalue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KeyToValueTest {
    private final KeyToValue<SinkRecord> xform = new KeyToValue<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemalessKeyIsNull() {
        xform.configure(Collections.singletonMap("fields.include", "a,b"));

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("a", 1);

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final HashMap<String, Integer> expectedValue = new HashMap<>();
        expectedValue.put("a", 1);

        Assert.assertNull(transformedRecord.keySchema());
        Assert.assertNull(transformedRecord.valueSchema());
        Assert.assertNull(transformedRecord.key());
        Assert.assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void schemaless() {
        Map<String, String> config = new HashMap<>(2);
        config.put("fields.include", "a,b");
        config.put("fields.exclude", "a");
        xform.configure(config);

        final HashMap<String, Integer> key = new HashMap<>();
        key.put("a", 1);
        key.put("b", 2);
        key.put("c", 3);

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("e", 1);

        final SinkRecord record = new SinkRecord("", 0, null, key, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final HashMap<String, Integer> expectedValue = new HashMap<>();
        expectedValue.put("b", 2);
        expectedValue.put("e", 1);

        Assert.assertNull(transformedRecord.keySchema());
        Assert.assertNull(transformedRecord.valueSchema());
        Assert.assertEquals(key, transformedRecord.key());
        Assert.assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void withSchema() {
        Map<String, String> config = new HashMap<>(2);
        config.put("fields.include", "a,b");
        config.put("fields.exclude", "a");
        xform.configure(config);

        final Schema keySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.INT32_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema);
        key.put("a", 1);
        key.put("b", 2);
        key.put("c", 3);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("e", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("e", 1);

        final SinkRecord record = new SinkRecord("", 0, keySchema, key, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Schema expectedValueSchema = SchemaBuilder.struct()
                .field("e", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();
        final Struct expectedValue = new Struct(expectedValueSchema)
                .put("e", 1)
                .put("b", 2);

        Assert.assertEquals(keySchema, transformedRecord.keySchema());
        Assert.assertEquals(key, transformedRecord.key());
        Assert.assertEquals(expectedValue, transformedRecord.value());
    }
}
