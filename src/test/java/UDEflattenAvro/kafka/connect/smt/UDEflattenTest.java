package UDEflattenAvro.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import static org.apache.kafka.connect.data.Schema.Type.MAP;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.junit.Assert.*;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class UDEflattenTest {


 //   private final UDEflatten<SourceRecord> xformKey = new UDEflatten.Key<>();
    private final UDEflatten<SourceRecord> xformValue = new UDEflatten.Value<>();

    @After
    public void teardown() {
//        xformKey.close();
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
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;


        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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

        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("stringMap", "stringy");
        HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A String",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        Struct transformedKeyStruct = (Struct) transformed.key();
        assertEquals(Schema.Type.STRUCT, transformed.keySchema().type());
        assertEquals("A String", transformedKeyStruct.getString("keyvalue"));
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
    public void testNestedStructNoBefore() {
        xformValue.configure(Collections.<String, String>emptyMap());
        xformValue.configure(Collections.singletonMap("BeforeImageTreat", 1));
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("stringMap", "stringy");
        HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        //build key default singleton string
        Map<String, String> key = Collections.singletonMap("A", "A key");
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A String",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(11, transformedStruct.schema().fields().size());
        try {
            // should be no before image
            assertNotEquals(16, (short) transformedStruct.getInt16("B_int16"));
            fail();
        } catch (Exception e) {

        }
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
    public void testNestedStructNoBeforeNotflat() {


        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("BeforeImageTreat", 1);
        config.put("doWrapkey", 2);
        config.put("doFlatten", false);
        xformValue.configure(config);
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage",supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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
        supportedTypes.put("string", "stringy");


        Struct supportedTypesA = new Struct(supportedTypesSchema);
        supportedTypesA.put("int8", (byte) 8);
        supportedTypesA.put("int16", (short) 16);
        supportedTypesA.put("int32", 32);
        supportedTypesA.put("int64", (long) 64);
        supportedTypesA.put("float32", 32.f);
        supportedTypesA.put("float64", 64.);
        supportedTypesA.put("boolean", true);
        supportedTypesA.put("string", "stringy");
        supportedTypesA.put("bytes", "bytes".getBytes());
        supportedTypesA.put("string", "Afterstringy");

        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", supportedTypesA);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("TransactionSequence", "stringy");
        HeaderTypes.put("SequenceTransaction","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        //build key default singleton string

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A String",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(10, transformedStruct.schema().fields().size());
        try {
            // should be no before image
            assertNotEquals(16, (short) transformedStruct.getInt16("B_int16"));
            fail();
        } catch (Exception e) {

        }
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("Afterstringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
        Struct transformedStruct1 = (Struct) transformed.key();
        assertEquals("stringy", transformedStruct1.getString("string"));


    }
    public void testEmptyBeforeNoBeforeNotflat() {


        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("BeforeImageTreat", 1);
        config.put("doWrapkey", 2);
        config.put("doFlatten", false);
        xformValue.configure(config);
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder ;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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

        Struct beforeImageStruct = new Struct(supportedTypesSchema);
        beforeImageStruct.put("BeforeImage", null);
        Struct afterImageStruct = new Struct(supportedTypesSchema);
        afterImageStruct.put("AfterImage", supportedTypes);
        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        //twoLevelNestedStruct.put("BeforeImage", beforeImageStruct);
       // twoLevelNestedStruct.put("AfterImage", afterImageStruct);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("TransactionSequence", "stringy");
        HeaderTypes.put("SequenceTransaction","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        //build key default singleton string

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A String",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(10, transformedStruct.schema().fields().size());
        try {
            // should be no before image
            assertNotEquals(16, (short) transformedStruct.getInt16("B_int16"));
            fail();
        } catch (Exception e) {

        }
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
    public void testMultipleKeys() {
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("doWrapkey", 2);
        xformValue.configure(config);
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        HeaderTypes.put("stringMap", "stringy");
        HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, null, "test",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(20, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("int8"));
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("stringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
        //now test wrapped keys
        Map<String,Object> config1 = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config1.put("doWrapkey", 2);
        xformValue.configure(config1);
        Struct supportedTypesBefore = new Struct(supportedTypesSchema);
        supportedTypesBefore.put("int8", (byte) 8);
        supportedTypesBefore.put("int16", (short) 16);
        supportedTypesBefore.put("int32", 32);
        supportedTypesBefore.put("int64", (long) 64);
        supportedTypesBefore.put("float32", 32.f);
        supportedTypesBefore.put("float64", 64.);
        supportedTypesBefore.put("boolean", true);
        supportedTypesBefore.put("string", "Beforestringy");
        supportedTypesBefore.put("bytes", "bytes".getBytes());

        Struct twoLevelNestedStruct1 = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct1.put("BeforeImage", supportedTypesBefore);
        twoLevelNestedStruct1.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes1 = new HashMap<>();
        HeaderTypes1.put("stringMap", "stringy");
        HeaderTypes1.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed1 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct1));
        assertEquals(Schema.Type.STRUCT, transformed1.valueSchema().type());
        assertEquals(Schema.Type.STRUCT, transformed1.keySchema().type());
        Struct transformedStruct1 = (Struct) transformed1.key();
        assertEquals("Beforestringy", transformedStruct1.getString("string"));
    }


    @Test
    public void testNullMapNestedStruct() {
        // test tombstone in Value
        xformValue.configure(Collections.<String, String>emptyMap());
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;
        //Schema beforeImageSchema = buildBeforeImage(supportedTypesSchema);
        //Schema afterImageSchema = buildAfterImage(supportedTypesSchema);
        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", null);
        Map<String, String> HeaderTypes = new HashMap<>();
        //HeaderTypes.put("stringMap", "stringy");
        //HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
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
        assertNull(transformedStruct.getInt8("int8"));
        assertNull(transformedStruct.getInt16("int16"));
        assertNull(transformedStruct.getInt32("int32"));
        assertNull(transformedStruct.getInt64("int64"));
        assertNull(transformedStruct.getFloat32("float32"));
        assertNull(transformedStruct.getFloat64("float64"));
        assertNull(transformedStruct.getBoolean("boolean"));
        assertNull(transformedStruct.getString("string"));
        assertNull(transformedStruct.getBytes("bytes"));
        //now test wrapped keys
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("doWrapkey", 2);
        xformValue.configure(config);

        Struct supportedTypesBefore = new Struct(supportedTypesSchema);
        supportedTypesBefore.put("int8", (byte) 8);
        supportedTypesBefore.put("int16", (short) 16);
        supportedTypesBefore.put("int32", 32);
        supportedTypesBefore.put("int64", (long) 64);
        supportedTypesBefore.put("float32", 32.f);
        supportedTypesBefore.put("float64", 64.);
        supportedTypesBefore.put("boolean", true);
        supportedTypesBefore.put("string", "Beforestringy");
        supportedTypesBefore.put("bytes", "bytes".getBytes());

        Struct twoLevelNestedStruct1 = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct1.put("BeforeImage", supportedTypesBefore);
        twoLevelNestedStruct1.put("AfterImage", null);
        Map<String, String> HeaderTypes1 = new HashMap<>();
        HeaderTypes1.put("stringMap", "stringy");
        HeaderTypes1.put("stringMapEmpty","");
        twoLevelNestedStruct1.put("Header",HeaderTypes);
        SourceRecord transformed1 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct1));
        assertEquals(Schema.Type.STRUCT, transformed1.valueSchema().type());
        assertEquals(Schema.Type.STRUCT, transformed1.keySchema().type());
        Struct transformedStruct1 = (Struct) transformed1.key();
        assertEquals("Beforestringy", transformedStruct1.getString("string"));

    }
    @Test
    public void testNullBeforeNestedStruct() {
        // Equivalent to Insert records, default options
        xformValue.configure(Collections.<String, String>emptyMap());

        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;
        //Schema beforeImageSchema = buildBeforeImage(supportedTypesSchema);
        //Schema afterImageSchema = buildAfterImage(supportedTypesSchema);
        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", null);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        //HeaderTypes.put("stringMap", "stringy");
        //HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(18, transformedStruct.schema().fields().size());
        assertNull(transformedStruct.getInt8("B_int8"));
        assertNull(transformedStruct.getInt16("B_int16"));
        assertNull(transformedStruct.getInt32("B_int32"));
        assertNull(transformedStruct.getInt64("B_int64"));
        assertNull(transformedStruct.getFloat32("B_float32"));
        assertNull(transformedStruct.getFloat64("B_float64"));
        assertNull(transformedStruct.getBoolean("B_boolean"));
        assertNull( transformedStruct.getString("B_string"));
        assertNull(transformedStruct.getBytes("B_bytes"));
        assertEquals(8, (byte) transformedStruct.getInt8("int8"));
        assertEquals(16, (short) transformedStruct.getInt16("int16"));
        assertEquals(32, (int) transformedStruct.getInt32("int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("int64"));
        assertEquals(32.f, transformedStruct.getFloat32("float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("boolean"));
        assertEquals("stringy", transformedStruct.getString("string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("bytes"));
        // test with primary keys
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("doWrapkey", 2);
        xformValue.configure(config);

        Struct supportedTypesAfter = new Struct(supportedTypesSchema);
        supportedTypesAfter.put("int8", (byte) 8);
        supportedTypesAfter.put("int16", (short) 16);
        supportedTypesAfter.put("int32", 32);
        supportedTypesAfter.put("int64", (long) 64);
        supportedTypesAfter.put("float32", 32.f);
        supportedTypesAfter.put("float64", 64.);
        supportedTypesAfter.put("boolean", true);
        supportedTypesAfter.put("string", "Afterstringy");
        supportedTypesAfter.put("bytes", "bytes".getBytes());

        Struct twoLevelNestedStruct1 = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct1.put("BeforeImage", null);
        twoLevelNestedStruct1.put("AfterImage",supportedTypesAfter);
        Map<String, String> HeaderTypes1 = new HashMap<>();
        HeaderTypes1.put("stringMap", "stringy");
        HeaderTypes1.put("stringMapEmpty","");
        twoLevelNestedStruct1.put("Header",HeaderTypes);
        SourceRecord transformed1 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct1));
        assertEquals(Schema.Type.STRUCT, transformed1.valueSchema().type());
        assertEquals(Schema.Type.STRUCT, transformed1.keySchema().type());
        Struct transformedStruct1 = (Struct) transformed1.key();
        assertEquals("Afterstringy", transformedStruct1.getString("string"));
    }
    @Test
    public void testNullBeforeNestedStructWrap() {
        // Equivalent to Insert records, default options
        xformValue.configure(Collections.<String, String>emptyMap());

        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);

        builder = SchemaBuilder.struct();
        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", null);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        //HeaderTypes.put("stringMap", "stringy");
        //HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);

        // test with primary keys
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("doWrapkey", 2);
        xformValue.configure(config);

        Struct supportedTypesAfter = new Struct(supportedTypesSchema);
        supportedTypesAfter.put("int8", (byte) 8);
        supportedTypesAfter.put("int16", (short) 16);
        supportedTypesAfter.put("int32", 32);
        supportedTypesAfter.put("int64", (long) 64);
        supportedTypesAfter.put("float32", 32.f);
        supportedTypesAfter.put("float64", 64.);
        supportedTypesAfter.put("boolean", true);
        supportedTypesAfter.put("string", "Afterstringy");
        supportedTypesAfter.put("bytes", "bytes".getBytes());

        Struct twoLevelNestedStruct1 = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct1.put("BeforeImage", null);
        twoLevelNestedStruct1.put("AfterImage", supportedTypesAfter);
        Map<String, String> HeaderTypes1 = new HashMap<>();
        HeaderTypes1.put("stringMap", "stringy");
        HeaderTypes1.put("stringMapEmpty","");
        twoLevelNestedStruct1.put("Header",HeaderTypes);
        SourceRecord transformed1 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct1));
        assertEquals(Schema.Type.STRUCT, transformed1.valueSchema().type());
        assertEquals(Schema.Type.STRUCT, transformed1.keySchema().type());
        Struct transformedStruct1 = (Struct) transformed1.key();
        assertEquals("Afterstringy", transformedStruct1.getString("string"));
    }
    @Test
    public void testNullBeforeWrapNoBefore() {
        // Equivalent to Insert records, default options
        //xformValue.configure(Collections.<String, String>emptyMap());

        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);

        builder = SchemaBuilder.struct().optional();

        builder.field("BeforeImage", supportedTypesSchema);
        builder.field("AfterImage",supportedTypesSchema);
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

        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", null);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        //HeaderTypes.put("stringMap", "stringy");
        //HeaderTypes.put("stringMapEmpty","");
        twoLevelNestedStruct.put("Header",HeaderTypes);

        // test with primary keys
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("pk.fields", Arrays.asList("string")));
        config.put("BeforeImageTreat", 1);
        config.put("doWrapkey", 2);
        xformValue.configure(config);

        Struct supportedTypesAfter = new Struct(supportedTypesSchema);
        supportedTypesAfter.put("int8", (byte) 8);
        supportedTypesAfter.put("int16", (short) 16);
        supportedTypesAfter.put("int32", 32);
        supportedTypesAfter.put("int64", (long) 64);
        supportedTypesAfter.put("float32", 32.f);
        supportedTypesAfter.put("float64", 64.);
        supportedTypesAfter.put("boolean", true);
        supportedTypesAfter.put("string", "Afterstringy");
        supportedTypesAfter.put("bytes", "bytes".getBytes());

        Struct twoLevelNestedStruct1 = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct1.put("BeforeImage", null);
        twoLevelNestedStruct1.put("AfterImage", supportedTypesAfter);
        Map<String, String> HeaderTypes1 = new HashMap<>();
        HeaderTypes1.put("stringMap", "stringy");
        HeaderTypes1.put("stringMapEmpty","");
        twoLevelNestedStruct1.put("Header",HeaderTypes);
        SourceRecord transformed1 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A key",
                twoLevelNestedSchema, twoLevelNestedStruct1));
        assertEquals(Schema.Type.STRUCT, transformed1.valueSchema().type());
        assertEquals(Schema.Type.STRUCT, transformed1.keySchema().type());
        Struct transformedStruct1 = (Struct) transformed1.key();
        assertEquals("Afterstringy", transformedStruct1.getString("string"));
    }
    @Test
    public void testNestedMapWithDelimiter() {
        xformValue.configure(Collections.singletonMap("delimiter", "#"));
        Schema supportedTypesSchema = buildSupportedTypesSchema();
        SchemaBuilder builder;

        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
        builder = SchemaBuilder.struct();
        builder.field("BeforeImage",supportedTypesSchema);
        builder.field("AfterImage", supportedTypesSchema);
        builder.field("Header", buildmap);
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


        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("BeforeImage", supportedTypes);
        twoLevelNestedStruct.put("AfterImage", supportedTypes);
        Map<String, String> HeaderTypes = new HashMap<>();
        twoLevelNestedStruct.put("Header", HeaderTypes);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, Schema.STRING_SCHEMA, "A Key",
                twoLevelNestedSchema, twoLevelNestedStruct));
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(18, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("B#int8"));
        assertEquals(16, (short) transformedStruct.getInt16("B#int16"));
        assertEquals(32, (int) transformedStruct.getInt32("B#int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("B#int64"));
        assertEquals(32.f, transformedStruct.getFloat32("B#float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("B#float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("B#boolean"));
        assertEquals("stringy", transformedStruct.getString("B#string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("B#bytes"));
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

    //No UDE message should be sent without a schema
    @Test(expected = DataException.class)
    public void tombstoneEventWithoutSchemaShouldPassThrough() {
        xformValue.configure(Collections.<String, String>emptyMap());

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);
        final SourceRecord transformedRecord = xformValue.apply(record);
    }

    @Test
    public void tombstoneEventWithSchemaShouldPassThrough1() {
        //keywrap to make valid JSON
        xformValue.configure(Collections.<String, String>emptyMap());
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final SourceRecord record = new SourceRecord(null, null, "test", 0,Schema.STRING_SCHEMA, "key field",
                simpleStructSchema, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }
    @Test
    public void tombstoneEventWithSchemaShouldPassThrough2() {
        //dont change the key
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("doWrapkey", 0));
        //config.put("doWrapkey", 0);
        xformValue.configure(config);
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final SourceRecord record = new SourceRecord(null, null, "test", 0,Schema.STRING_SCHEMA, "key field",
                simpleStructSchema, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }
    @Test(expected = DataException.class)
    public void tombstoneEventWithSchemaShouldPassThrough3() {
        //Use before image not possible with afterimage only
        Map<String,Object> config = new HashMap<String,Object>(Collections.singletonMap("doWrapkey", 2));
        config.put("pk.fields", Arrays.asList("magic"));
        xformValue.configure(config);
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final SourceRecord record = new SourceRecord(null, null, "test", 0,Schema.STRING_SCHEMA, "key field",
                simpleStructSchema, null);
        final SourceRecord transformedRecord = xformValue.apply(record);

    }
    public Schema buildSupportedTypesSchema() {
        SchemaBuilder builder = SchemaBuilder.struct().optional();
        builder.field("int8", Schema.OPTIONAL_INT8_SCHEMA);
        builder.field("int16", Schema.OPTIONAL_INT16_SCHEMA);
        builder.field("int32", Schema.OPTIONAL_INT32_SCHEMA);
        builder.field("int64", Schema.OPTIONAL_INT64_SCHEMA);
        builder.field("float32", Schema.OPTIONAL_FLOAT32_SCHEMA);
        builder.field("float64", Schema.OPTIONAL_FLOAT64_SCHEMA);
        builder.field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA);
        builder.field("string", Schema.OPTIONAL_STRING_SCHEMA);
        builder.field("bytes", Schema.OPTIONAL_BYTES_SCHEMA);
        return builder.build();
    }



}