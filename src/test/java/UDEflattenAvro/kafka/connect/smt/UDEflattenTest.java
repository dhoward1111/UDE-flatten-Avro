package UDEflattenAvro.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import static org.apache.kafka.connect.data.Schema.Type.MAP;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class UDEflattenTest {


    private final UDEflatten<SourceRecord> xformKey = new UDEflatten.Key<>();
    private final UDEflatten<SourceRecord> xformValue = new UDEflatten.Value<>();

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }


    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xformValue.configure(Collections.<String, String>emptyMap());
        xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test(expected = DataException.class)
    public void topLevelMapRequired() {
        xformValue.configure(Collections.<String, String>emptyMap());
        xformValue.apply(new SourceRecord(null, null, "topic", 0, null, 42));
    }

    @Test
    public void testNestedStruct() {
        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.INT16_SCHEMA);
        builder.field("int32", Schema.INT32_SCHEMA);
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("bytes", Schema.BYTES_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        Schema beforeImageSchema = builder.build();
        builder = SchemaBuilder.struct();
        builder.field("AfterImage", supportedTypesSchema);
        Schema afterImageSchema = builder.build();
        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", beforeImageSchema);
        builder.field("AfterImage",afterImageSchema);
        builder.field("Header",buildmap);
        Schema twoLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Struct beforeImageStruct = new Struct(beforeImageSchema);
        beforeImageStruct.put("BeforeImage", supportedTypes);
        Struct afterImageStruct = new Struct(afterImageSchema);
        afterImageStruct.put("AfterImage", supportedTypes);
        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", beforeImageStruct);
        twoLevelNestedStruct.put("AfterImage", afterImageStruct);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("stringMap", "stringy");
        HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(20, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("B_int8"));
        assertEquals(16, (short) transformedStruct.getInt16("B_int16"));
        assertEquals(32, (int) transformedStruct.getInt32("B_int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("B_int64"));
        assertEquals(32.f, transformedStruct.getFloat32("B_float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("B_float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("B_boolean"));
        assertEquals("stringy", transformedStruct.getString("B_string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("B_bytes"));
        assertEquals(8, (byte) transformedStruct.getInt8("int8"));
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("stringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
    }
    @Test
    public void testNoBeforeImage() {
        xformValue.configure(Collections.singletonMap("BeforeImageTreat", 1));


        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.INT16_SCHEMA);
        builder.field("int32", Schema.INT32_SCHEMA);
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("bytes", Schema.BYTES_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        Schema beforeImageSchema = builder.build();
        builder = SchemaBuilder.struct();
        builder.field("AfterImage", supportedTypesSchema);
        Schema afterImageSchema = builder.build();
        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", beforeImageSchema);
        builder.field("AfterImage",afterImageSchema);
        builder.field("Header",buildmap);
        Schema twoLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Struct beforeImageStruct = new Struct(beforeImageSchema);
        beforeImageStruct.put("BeforeImage", supportedTypes);
        Struct afterImageStruct = new Struct(afterImageSchema);
        afterImageStruct.put("AfterImage", supportedTypes);
        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", beforeImageStruct);
        twoLevelNestedStruct.put("AfterImage", afterImageStruct);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("stringMap", "stringy");
        HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(11, transformedStruct.schema().fields().size());

        assertEquals(8, (byte) transformedStruct.getInt8("int8"));
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("stringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
    }
    @Test
    public void testNullMapNestedStruct() {
        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.INT16_SCHEMA);
        builder.field("int32", Schema.INT32_SCHEMA);
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("bytes", Schema.BYTES_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        Schema beforeImageSchema = builder.build();
        builder = SchemaBuilder.struct();
        builder.field("AfterImage", supportedTypesSchema);
        Schema afterImageSchema = builder.build();
        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", beforeImageSchema);
        builder.field("AfterImage",afterImageSchema);
        builder.field("Header",buildmap);
        Schema twoLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Struct beforeImageStruct = new Struct(beforeImageSchema);
        beforeImageStruct.put("BeforeImage", supportedTypes);
        Struct afterImageStruct = new Struct(afterImageSchema);
        afterImageStruct.put("AfterImage", supportedTypes);
        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", beforeImageStruct);
        twoLevelNestedStruct.put("AfterImage", afterImageStruct);
        Map<String, String> HeaderTypes = new HashMap<>();
        //HeaderTypes.put("stringMap", "stringy");
        //HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(18, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("B_int8"));
        assertEquals(16, (short) transformedStruct.getInt16("B_int16"));
        assertEquals(32, (int) transformedStruct.getInt32("B_int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("B_int64"));
        assertEquals(32.f, transformedStruct.getFloat32("B_float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("B_float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("B_boolean"));
        assertEquals("stringy", transformedStruct.getString("B_string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("B_bytes"));
        assertEquals(8, (byte) transformedStruct.getInt8("int8"));
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("stringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
    }
    @Test
    public void testNestedMapWithDelimiter() {
        xformValue.configure(Collections.singletonMap("delimiter", "#"));

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", (Object) supportedTypes);
        Map<String, Object> twoLevelNestedMap = Collections.singletonMap("A", (Object) oneLevelNestedMap);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                null, twoLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertTrue(transformed.value() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();
        assertEquals(9, transformedMap.size());
        assertEquals((byte) 8, transformedMap.get("A#B#int8"));
        assertEquals((short) 16, transformedMap.get("A#B#int16"));
        assertEquals(32, transformedMap.get("A#B#int32"));
        assertEquals((long) 64, transformedMap.get("A#B#int64"));
        assertEquals(32.f, (float) transformedMap.get("A#B#float32"), 0.f);
        assertEquals(64., (double) transformedMap.get("A#B#float64"), 0.);
        assertEquals(true, transformedMap.get("A#B#boolean"));
        assertEquals("stringy", transformedMap.get("A#B#string"));
        assertArrayEquals("bytes".getBytes(), (byte[]) transformedMap.get("A#B#bytes"));
    }

    @Test
    public void testOptionalFieldStruct() {
        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("opt_int32", null);

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", supportedTypes);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertNull(transformedStruct.get("B_opt_int32"));
    }

    @Test
    public void testOptionalStruct() {
        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema schema = builder.build();

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                schema, null));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        assertNull(transformed.value());
    }

    @Test
    public void testOptionalNestedStruct() {
        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", null);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertNull(transformedStruct.get("B_opt_int32"));
    }

    @Test
    public void testOptionalFieldMap() {
        xformValue.configure(Collections.<String, String>emptyMap());

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("opt_int32", null);

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", (Object) supportedTypes);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0,
                null, oneLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertTrue(transformed.value() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();

        assertNull(transformedMap.get("B.opt_int32"));
    }

    @Test
    public void testKey() {
        xformKey.configure(Collections.<String, String>emptyMap());

        Map<String, Map<String, Integer>> key = Collections.singletonMap("A", Collections.singletonMap("B", 12));
        SourceRecord src = new SourceRecord(null, null, "topic", null, key, null, null);
        SourceRecord transformed = xformKey.apply(src);

        assertNull(transformed.keySchema());
        assertTrue(transformed.key() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.key();
        assertEquals(12, transformedMap.get("A_B"));
    }

    @Test(expected = DataException.class)
    public void testUnsupportedTypeInMap() {
        xformValue.configure(Collections.<String, String>emptyMap());
        Object value = Collections.singletonMap("foo", Arrays.asList("bar", "baz"));
        xformValue.apply(new SourceRecord(null, null, "topic", 0, null, value));
    }

    @Test
    public void testOptionalAndDefaultValuesNested() {
        // If we have a nested structure where an entire sub-Struct is optional, all flattened fields generated from its
        // children should also be optional. Similarly, if the parent Struct has a default value, the default value for
        // the flattened field

        xformValue.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("req_field", Schema.STRING_SCHEMA);
        builder.field("opt_field", SchemaBuilder.string().optional().defaultValue("child_default").build());
        Struct childDefaultValue = new Struct(builder);
        childDefaultValue.put("req_field", "req_default");
        builder.defaultValue(childDefaultValue);
        Schema schema = builder.build();
        // Intentionally leave this entire value empty since it is optional
        Struct value = new Struct(schema);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, schema, value));

        assertNotNull(transformed);
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRUCT, transformedSchema.type());
        assertEquals(2, transformedSchema.fields().size());
        // Required field should pick up both being optional and the default value from the parent
        Schema transformedReqFieldSchema = SchemaBuilder.string().optional().defaultValue("req_default").build();
        assertEquals(transformedReqFieldSchema, transformedSchema.field("req_field").schema());
        // The optional field should still be optional but should have picked up the default value. However, since
        // the parent didn't specify the default explicitly, we should still be using the field's normal default
        Schema transformedOptFieldSchema = SchemaBuilder.string().optional().defaultValue("child_default").build();
        assertEquals(transformedOptFieldSchema, transformedSchema.field("opt_field").schema());
    }

    @Test
    public void tombstoneEventWithoutSchemaShouldPassThrough() {
        xformValue.configure(Collections.<String, String>emptyMap());

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(null, transformedRecord.value());
        assertEquals(null, transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneEventWithSchemaShouldPassThrough() {
        xformValue.configure(Collections.<String, String>emptyMap());

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                simpleStructSchema, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(null, transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }
}