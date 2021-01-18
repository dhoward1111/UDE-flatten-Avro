package UDEflattenAvro.kafka.connect.smt;

/*
 * Copyright © 2020 David Howard (David@ukhoward.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Based on the org.apache.kafka.connect.transforms.Flatten
 */



import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.*;

public abstract class UDEflatten<R extends ConnectRecord<R>> implements Transformation<R> {


    public static final String OVERVIEW_DOC =
            "Flatten the Unisys Data Exchange nested data structure, generating names for each field by concatenating the field names at each "
                    + "level with a configurable delimiter character. Applies to Struct when schema present, or a Map "
                    + "in the case of schemaless data. The default delimiter is '.'."
                    + "The BeforeImage can be made optional and BeforeImage is shortened to B"
                    + "The AfterImage fields have no prefix by default "
                    + "<p/>Use the concrete transformation type designed for the record key  "
                    + "and value (<code>" + Value.class.getName() + "</code>)."
                    + "it will also make the key a valid JSON structure";

    private static final String DELIMITER_CONFIG = "delimiter";
    private static final String DELIMITER_DEFAULT = "_";
    private static final String BEFOREIMAGETREAT_CONFIG = "BeforeImageTreat";
    private static final Integer BEFOREIMAGETREAT_DEFAULT = 0; //
    private static final String BEFOREIMAGE_CONFIG = "BeforeImage-Prefix";
    private static final String BEFOREIMAGE_DEFAULT = "B"; //
    private static final String WRAPKEY_CONFIG = "wrapkeyElement";
    private static final String WRAPKEY_DEFAULT = "keyvalue"; //
    private static final String DOWRAPKEY_CONFIG = "doWrapkey";
    private static final int DOWRAPKEY_DEFAULT = 1;
    private static final String DOFLATTEN_CONFIG = "doFlatten";
    private static final boolean DOFLATTEN_DEFAULT = true;
    private static final String ENABLETOMB_CONFIG = "enableTomb";
    private static final boolean ENABLETOMB_DEFAULT = true;
    public static final String PK_FIELDS_CONFIG = "pk.fields";
    private static final String PK_FIELDS_DEFAULT = "";
    private static final String PK_FIELDS_DOC =
            "List of comma-separated primary key field names. The runtime interpretation of this config"
                    + "    if no fields specified then the key in the message is used ";
    private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DOFLATTEN_CONFIG , ConfigDef.Type.BOOLEAN, DOFLATTEN_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Enter the name of the keyvalue element - default keyvalue ")
            .define(DELIMITER_CONFIG, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Delimiter to insert between field names from the input record when generating field names for the "
                            + "output record")
            .define(BEFOREIMAGETREAT_CONFIG , ConfigDef.Type.INT, BEFOREIMAGETREAT_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "How to treat BeforeImage, 0 - include, 1 - Exclude ")
            .define(BEFOREIMAGE_CONFIG , ConfigDef.Type.STRING, BEFOREIMAGE_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Rename the Before Image - default B ")
            .define(DOWRAPKEY_CONFIG , ConfigDef.Type.INT, DOWRAPKEY_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "0 - Do nothing to the key "
                    + "1 - Use the value in PK Fields to build single key value of type string"
                    + "2 - Use the list of PK fields and their types to build a key structure ")
            .define(ENABLETOMB_CONFIG , ConfigDef.Type.BOOLEAN, ENABLETOMB_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "True (default) - propagate a tombstone record"
                            + "False - do not propagate a tombstone record, leave overall value structure")
              .define(PK_FIELDS_CONFIG, ConfigDef.Type.LIST, PK_FIELDS_DEFAULT, ConfigDef.Importance.MEDIUM,
                    PK_FIELDS_DOC );

    private static final String PURPOSE = "flattening UDE structure";

    private Set<String> metaData =
            new HashSet<String>(Arrays.asList("TransactionSequence",
                    "SequenceTransaction",
                    "TransactionID",
                    "Timestamp",
                    "Operation"));
    private String delimiter;
    private Integer beforeImageTreat;
    private String beforeImage;
    private Integer doKeywrap;
    private boolean doFlatten;
    private boolean enableTomb;
    private String keyWrapElement;


    private Cache<Schema, Schema> schemaUpdateCache;
    private Cache<Schema, Schema> wrapKeySchemaCache;
    private Cache<Schema, Schema> schemaNoBeforeCache;
    private List<String> pkFields = new ArrayList<String>();

    private Schema keySchema = null;
    private boolean hasBefore = false;
    private boolean tombStone = false;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        delimiter = config.getString(DELIMITER_CONFIG);
        beforeImageTreat = config.getInt(BEFOREIMAGETREAT_CONFIG);
        beforeImage = config.getString(BEFOREIMAGE_CONFIG);
        doKeywrap = config.getInt(DOWRAPKEY_CONFIG);
        doFlatten = config.getBoolean(DOFLATTEN_CONFIG);
        enableTomb = config.getBoolean(ENABLETOMB_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        wrapKeySchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
        schemaNoBeforeCache = new SynchronizedCache<>(new LRUCache<>(16));
        pkFields.clear();
        pkFields.addAll(config.getList(PK_FIELDS_CONFIG));
        switch (doKeywrap) {
            case 0:
                if (pkFields.size() != 0) {
                    throw new ConfigException(
                            "Cannot specify a primary key name and not process the key");
                }
                break;
            case 1:
                if (pkFields.size() > 1) {
                    throw new ConfigException(
                            "Only One primary key name needs to be defined");
                }
                if (pkFields.size() == 0) pkFields.add(WRAPKEY_DEFAULT);
                keyWrapElement = pkFields.get(0);
                break;
            case 2:
                if (pkFields.size() == 0) {
                    throw new ConfigException(
                            "At least one primary key name needs to be defined");
                }
                break;
            default:
                throw new ConfigException(
                        "Key handling value must be either 0,1 or 2");
        }
    }


    @Override
    public R apply(R record) {
        R result;
        // probably should wrap these into a class to allow return from functions
        keySchema=null;
        hasBefore=false;
        tombStone = false;
        if (operatingSchema(record) == null) {
            throw new DataException("Requires Avro Formatted Data");
        } else {
             // Flatten input record first then process the returned values
             // need to check if empty value and tombstone
            result = applyWithSchema(record);
            keySchema = buildKeySchema(result.valueSchema());
            if (doFlatten) {
                if (beforeImageTreat==0 & !tombStone) {
                    return applyKeyWrapWithSchema(result, result);
                } else { // remove beforeImage
                    return applyKeyWrapWithSchema(removeBefore(result),result);
                }
            } else {
                if (beforeImageTreat==0 & !tombStone) {
                    return applyKeyWrapWithSchema(record,result);
                } else { // remove beforeImage
                    return applyKeyWrapWithSchema(removeBefore(record),result);
                }
             }
        }
    }

    @Override
    public void close() {
    }
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R removeBefore(R inRecord) {
        final Struct value = requireStructOrNull(operatingValue(inRecord), PURPOSE);
        Schema schema = operatingSchema(inRecord);
        Schema updatedSchema = schemaNoBeforeCache.get(schema);

        if (updatedSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            Struct defaultValue = (Struct) schema.defaultValue();
            buildNoBeforeSchema(schema,  builder,  schema.isOptional(), defaultValue);
            updatedSchema = builder.build();
            schemaNoBeforeCache.put(schema, updatedSchema);
        }
        Struct newRecord = new Struct(updatedSchema);
        Struct invalue = requireStructOrNull(operatingValue(inRecord), PURPOSE);
       if (doFlatten) {
           if (tombStone) {
               return newRecord(inRecord,updatedSchema,null);
           } else
           {
            for (Field field : updatedSchema.fields()) {  //take fields from Inrecord
                newRecord.put(field, invalue.get(field.name()));
            }
           }
        } else { //inRecord will be the layout sent we need AfterImage and Header
           if (tombStone) {
               return newRecord(inRecord,updatedSchema,null);
           } else {
               for (Field field : invalue.schema().fields()) {
                   //top level
                   if (field.name().equals("AfterImage")) {
                       if (doFlatten) {
                           Schema innerSchema = field.schema();
                           Struct innerStruct = invalue.getStruct(field.name());
                           for (Field innerField : innerSchema.fields()) {
                               newRecord.put(innerField.name(), innerStruct.get(innerField));
                           }
                       } else {
                           newRecord.put(field.name(), invalue.get(field.name()));
                       }

                   }
                   if (field.name().equals("Header")) {
                       newRecord.put(field.name(), invalue.getMap(field.name()));
                   }
               }
           }
        }
        return newRecord(inRecord,updatedSchema,newRecord);
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

        Schema schema = operatingSchema(record);
        Schema updatedSchema = schemaUpdateCache.get(schema);
         if (updatedSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            Struct defaultValue = (Struct) schema.defaultValue();
            buildUpdatedSchema(schema, "", builder,  schema.isOptional(), defaultValue);
            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }
        if (value == null) {
            //this will only happen if there is no before image and single key
            return newRecord(record, updatedSchema, null);
        } else {
            boolean fin;
            int cnt = 0;
            Struct updatedValue = null;
            do {
                fin = true;
                cnt++; //just to stop loops
                try { //the MAP structure is a pain
                    updatedSchema = schemaUpdateCache.get(schema);
                    updatedValue = new Struct(updatedSchema);
                    buildWithSchema(schema, updatedSchema,  value, "", updatedValue);
                } catch (Exception e) {
                    fin = false;
                }
            } while (cnt < 10 & !fin);
            if (cnt == 10) {
                throw new DataException("UDEFlatten had issue with MAP structure ");
            }
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Schema buildKeySchema (Schema inRecord) {
 // builds the key structure
        final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        switch (doKeywrap) {
            case 0:
                return null;
            case 1:
                Schema keySchema = wrapKeySchemaCache.get(inRecord);
                if (keySchema == null) {
                    keySchemaBuilder.field(keyWrapElement, Schema.OPTIONAL_STRING_SCHEMA);
                    keySchema = keySchemaBuilder.build();
                    wrapKeySchemaCache.put(inRecord, keySchema);
                }
                return keySchema;
            case 2:
                Schema keySchema1 = wrapKeySchemaCache.get(inRecord);
                if (keySchema1 == null) {
                    // work through the PKlist / either in the record schema or in the map structure
                    for (String fieldName : pkFields) {
                        Field field = inRecord.field(fieldName);
                        Schema fieldSchema;
                        if (field == null) {
                            fieldSchema = processMapName(fieldName);
                        } else {
                            fieldSchema = field.schema();
                        }
                        if (fieldSchema == null) throw new DataException("Unknown Primary Key: " + fieldName);
                        keySchemaBuilder.field(fieldName,fieldSchema);
                    }
                    keySchema1 = keySchemaBuilder.build();
                    wrapKeySchemaCache.put(inRecord, keySchema1);
                }
                return keySchema1;
            default:
                throw new IllegalStateException("Unexpected value in Building Key Processing: " + doKeywrap);
        }
    }

    private Schema processMapName(String fieldName) {
        if (metaData.contains(fieldName))
            return Schema.OPTIONAL_STRING_SCHEMA;

        throw new DataException("Unexpected field name in Primary Keys: " + fieldName);

    }

    private R applyKeyWrapWithSchema(R record, R flatRecord) {
        // wraps the UDE generated key field
        final Struct value = requireStructOrNull(record.value(), PURPOSE);
        switch (doKeywrap) {
            case 0: // do nothing
                return record;
            case 1: // use key value from the record
                final Struct key = new Struct(keySchema);
                key.put(keyWrapElement, record.key());
                if (value != null) {
                    return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, value.schema(), value, record.timestamp());
                } else {
                    return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, record.valueSchema(), null, record.timestamp());
                }
            case 2:

                final Struct keyC = new Struct(keySchema);
                Struct flatvalue = requireStructOrNull(operatingValue(flatRecord), PURPOSE);
                if (flatvalue == null) {
                    throw new DataException("Need Before Image to process Primary Keys");
                }
                for (Field field : keySchema.fields()) {
                    if (hasBefore) {
                        String fieldName = fieldName(beforeImage, field.name());
                        keyC.put(field.name(), flatvalue.get(fieldName));
                    } else {
                        keyC.put(field.name(), flatvalue.get(field.name()));
                    }
                }
                if (value != null) {
                    return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, keyC, value.schema(), value, record.timestamp());
                } else {
                    return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, keyC, record.valueSchema(), null, record.timestamp());
                }
            }
        return null;
    }

    /**
     * Build an updated Struct Schema which flattens all nested fields into a single struct, handling cases where
     * optionality and default values of the flattened fields are affected by the optionality and default values of
     * parent/ancestor schemas (e.g. flattened field is optional because the parent schema was optional, even if the
     * schema itself is marked as required).
     * @param schema the schema to translate
     * @param fieldNamePrefix the prefix to use on field names, i.e. the delimiter-joined set of ancestor field names
     * @param newSchema the flattened schema being built
     * @param optional true if any ancestor schema is optional
     * @param defaultFromParent the default value, if any, included via the parent/ancestor schemas
     */

    private void buildUpdatedSchema(Schema schema, String fieldNamePrefix, SchemaBuilder newSchema,  boolean optional, Struct defaultFromParent) {

        for (Field field : schema.fields()) {
            if (!field.name().equals("Header"))  {
                // ignore header field as this will be handled once the data is known
                final String fieldName = fieldName(fieldNamePrefix, field.name());
                final boolean fieldIsOptional = optional || field.schema().isOptional();
                Object fieldDefaultValue = null;
                if (field.schema().defaultValue() != null) {
                    fieldDefaultValue = field.schema().defaultValue();
                } else if (defaultFromParent != null) {
                    fieldDefaultValue = defaultFromParent.get(field);
                }
                switch (field.schema().type()) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT32:
                    case FLOAT64:
                    case BOOLEAN:
                    case STRING:
                    case BYTES:
                            newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue));
                         break;
                    case STRUCT:
                        switch (field.name()) {
                            case "BeforeImage":
                                    buildUpdatedSchema(field.schema(), beforeImage, newSchema, fieldIsOptional, (Struct) fieldDefaultValue);
                                break;
                            case "AfterImage":
                                buildUpdatedSchema(field.schema(), "", newSchema, fieldIsOptional, (Struct) fieldDefaultValue);
                                break;
                            default:
                                buildUpdatedSchema(field.schema(), fieldName, newSchema, fieldIsOptional, (Struct) fieldDefaultValue);
                        }
                        break;
                    default:
                        throw new DataException("Flatten transformation does not support " + field.schema().type()
                                + " for record with schemas (for field " + fieldName + ").");
                }
            }
        }
    }

    private void buildNoBeforeSchema(Schema schema, SchemaBuilder newSchema,  boolean optional, Struct defaultFromParent) {
        for (Field field : schema.fields()) {
                final boolean fieldIsOptional = optional || field.schema().isOptional();
                Object fieldDefaultValue = null;
                Object innerFieldDefaultValue = null;
                if (field.schema().defaultValue() != null) {
                    fieldDefaultValue = field.schema().defaultValue();
                } else if (defaultFromParent != null) {
                    fieldDefaultValue = defaultFromParent.get(field);
                }
                switch (field.schema().type()) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT32:
                    case FLOAT64:
                    case BOOLEAN:
                    case STRING:
                    case BYTES:
                        if (!field.name().startsWith(beforeImage+delimiter)) {
                            newSchema.field(field.name(), convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue));
                        }
                        break;
                    case STRUCT:
                        switch (field.name()) {
                            case "BeforeImage": //remove
                                break;
                            case "AfterImage": //take AfterImage out if flatten
                                if (doFlatten) {
                                    Schema innerSchema = field.schema();
                                    for (Field innerField : innerSchema.fields()) {
                                        if (innerField.schema().defaultValue() != null) {
                                            innerFieldDefaultValue = innerField.schema().defaultValue();
                                        } else if (defaultFromParent != null) {
                                            innerFieldDefaultValue = defaultFromParent.get(innerField);
                                        }
                                        newSchema.field(innerField.name(), convertFieldSchema(innerField.schema(), fieldIsOptional, innerFieldDefaultValue));
                                    }
                                } else {
                                    newSchema.field(field.name(),field.schema());
                                }
                                break;

                            default:
                                throw new DataException("Building no before image structure gave error " + field.schema().type()
                                        + "  (for field " + field.name() + ").");
                        }
                        break;
                    case MAP:

                        SchemaBuilder buildmap = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA);
                        newSchema.field(field.name(),buildmap);

                        break;
                    default:
                        throw new DataException("Flatten transformation does not support " + field.schema().type()
                                + " for record with schemas (for field " + field.name() + ").");
                }
            }
        }


    private void addHeaderSchema (String fieldName, Schema targetSchema, Schema origSchema) {
        //need to be able to add mapped metadata fields

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(targetSchema, SchemaBuilder.struct());
        for (Field field: targetSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        final Schema newSchema = builder.build();
        schemaUpdateCache.put(origSchema, newSchema);
    }

    /**
     * Convert the schema for a field of a Struct with a primitive schema to the schema to be used for the flattened
     * version, taking into account that we may need to override optionality and default values in the flattened version
     * to take into account the optionality and default values of parent/ancestor schemas
     * @param orig the original schema for the field
     * @param optional whether the new flattened field should be optional
     * @param defaultFromParent the default value either taken from the existing field or provided by the parent
     */
    private Schema convertFieldSchema(Schema orig, boolean optional, Object defaultFromParent) {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(orig);
        if (optional)
            builder.optional();
        if (defaultFromParent != null)
            builder.defaultValue(defaultFromParent);
        return builder.build();
    }

    private void buildWithSchema(Schema origSchema, Schema targetSchema, Struct record, String fieldNamePrefix, Struct newRecord) {
        if (record == null) {
            return;
        }

        for (Field field : record.schema().fields()) {
            final String fieldName = fieldName(fieldNamePrefix, field.name());
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newRecord.put(fieldName, record.get(field));
                    break;
                case STRUCT:
                         switch (field.name()) {
                            case "BeforeImage":
                                   if (record.getStruct(field.name()) != null) hasBefore = true;
                                    buildWithSchema(origSchema, targetSchema, record.getStruct(field.name()), beforeImage, newRecord);
                                break;
                            case "AfterImage":
                                if (record.get(field.name()) == null) {
                                    //tombstone if enabled
                                    if (enableTomb) tombStone=true;
                                }
                                buildWithSchema(origSchema, targetSchema, record.getStruct(field.name()), "", newRecord);
                                break;
                            case "Header":
                                break;
                            default:
                                buildWithSchema(origSchema, targetSchema, record.getStruct(field.name()), fieldName, newRecord);
                        }
                    break;
                case MAP:
                    // work through MAP any fields not in record are added, no easy way to find if field not there
                    // so cause an exception, update schema, then exit and restart processing record, this will happen until
                    // all meta data items are processed
                    Map<String, String> headerTypes = record.getMap(field.name());
                    if (headerTypes!=null) {
                        for (Map.Entry<String, String> entry : headerTypes.entrySet()) {
                            try {
                                newRecord.put(entry.getKey(), entry.getValue());
                            } catch (Exception e) {
                                addHeaderSchema(entry.getKey(), targetSchema, origSchema);
                                throw new DataException("Adding new MetaDataField " + entry.getKey());
                            }
                        }
                    }
                    break;
                default:
                    throw new DataException("Flatten transformation does not support " + field.schema().type()
                            + " for record with schemas (for field " + fieldName + ").");
            }
        }


    }

    private String fieldName(String prefix, String fieldName) {
        return prefix.isEmpty() ? fieldName : (prefix + delimiter + fieldName);
    }

    public static class Value<R extends ConnectRecord<R>> extends UDEflatten<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
