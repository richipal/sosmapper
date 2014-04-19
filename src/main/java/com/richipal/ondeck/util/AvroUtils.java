package com.richipal.ondeck.util;


import com.richipal.ondeck.schema.avro.MasterSchema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created by rsingh on 4/19/14.
 */
public class AvroUtils {

  public static Schema.Type getAvroFieldType(Schema schema, String fieldName) throws IOException {

    if (!schemaContainsField(schema, fieldName)) {
      throw new IOException(
          "AvroUtils.getAvroFieldType: fieldName:" + fieldName
          + " not contained in schema:" + schema);
    }

    Schema.Type avroAttributeSchemaType = null;

    try {
      List<Schema> list = schema.getField(fieldName).schema().getTypes();

      for (Schema fieldSchema : list) {
        if (fieldSchema.getType() != Schema.Type.NULL) {
          if (avroAttributeSchemaType != null) {
            throw new IOException(
                "Encountered more than one non-null type in the fieldName:"
                    + fieldName
                    + ", but we currently only offer support for single-typed fields");
          }
          avroAttributeSchemaType = fieldSchema.getType(); 
        }
      }
    } catch (AvroRuntimeException e) {
      // not a list
      avroAttributeSchemaType = schema.getField(fieldName).schema().getType();
    }
    return avroAttributeSchemaType;
  }

  public static void setDatumAttribute(SpecificRecord datum, String fieldName, Object val) throws AvroTypeException, IOException {

    // TODO: This is evil - figure out how to precache these calls
    Schema schema = ReflectData.get().getSchema(MasterSchema.class);

    ConvertUtils.register(new IntegerConverter(null), Integer.class);
    ConvertUtils.register(new LongConverter(null), Long.class);

    if (schema.getField(fieldName) == null) {
      throw new IOException(
          "AvroUtils.setDatumAttribute field:" + fieldName
          + " does not exist in schema:" + schema);
    }

    if (val == null || (val instanceof String && val.equals(""))) {
      try {
        BeanUtils.setProperty(datum, fieldName, null);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new IOException();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
        throw new IOException();
      }
      return;
    }

    Schema.Type avroAttributeSchemaType = getAvroFieldType(schema, fieldName);

    /**
     Validate that the value being passed can actually be set in the Avro
     datum;  If it can't, throw an AvroTypeException;
     */
    if (avroAttributeSchemaType == Schema.Type.STRING) {// Everything can be autoboxed to a string
    } else if (avroAttributeSchemaType == Schema.Type.INT) {
      if (!(val instanceof Integer)) {
        try {
          Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
          throw new AvroTypeException(
              "'" + val.toString() + "' is not an int (fieldName:" + fieldName
              + ")");
        }
      }
    } else if (avroAttributeSchemaType == Schema.Type.LONG) {
      if (!(val instanceof Long)) {
        try {
          Long.parseLong(val.toString());
        } catch (NumberFormatException e) {
          throw new AvroTypeException(
              "'" + val.toString() + "' is not a long (fieldName:" + fieldName
              + ")");
        }
      }
    } else if (avroAttributeSchemaType == Schema.Type.BOOLEAN) {
      if (!(val instanceof Boolean)) {
        try {
          Boolean.parseBoolean(val.toString());
        } catch (Exception e) {
          throw new AvroTypeException(
              "'" + val.toString() + "' is not a boolean (fieldName:"
              + fieldName + ")");
        }
      }
    } else {
      throw new AvroTypeException("Schema.Type unsupported");
    }

    try {
      BeanUtils.setProperty(datum, fieldName, val);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      throw new IOException();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
      throw new IOException();
    }
  }
  
  public static boolean schemaContainsField(Schema schema, String fieldName) {
    return (schema.getField(fieldName) != null) ? true : false;
  }

}

