/*
 * Copyright (2023) The Delta Lake Project Authors.
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
 */
package io.delta.kafka.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kafka.DeltaSinkConfig;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

class RecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final StructType tableSchema;
  private final DeltaSinkConfig config;

  RecordConverter(StructType tableSchema, DeltaSinkConfig config) {
    this.tableSchema = tableSchema;
    this.config = config;
  }

  Row convert(Object data) {
    if (data instanceof Struct || data instanceof Map) {
      return convertStructValue(data, tableSchema);
    }
    throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
  }

  private Object convertValue(Object value, DataType type) {
    if (value == null) {
      return null;
    }

    if (type instanceof StructType) {
      return convertStructValue(value, (StructType) type);
    } else if (type instanceof ArrayType) {
      return convertListValue(value, (ArrayType) type);
    } else if (type instanceof MapType) {
      return convertMapValue(value, (MapType) type);
    } else if (type instanceof IntegerType) {
      return convertInt(value);
    } else if (type instanceof LongType) {
      return convertLong(value);
    } else if (type instanceof FloatType) {
      return convertFloat(value);
    } else if (type instanceof DoubleType) {
      return convertDouble(value);
    } else if (type instanceof DecimalType) {
      return convertDecimal(value, (DecimalType) type);
    } else if (type instanceof BooleanType) {
      return convertBoolean(value);
    } else if (type instanceof StringType) {
      return convertString(value);
    } else if (type instanceof BinaryType) {
      return convertBase64Binary(value);
    } else if (type instanceof DateType) {
      return convertDateValue(value);
    } else if (type instanceof TimestampType) {
      return convertTimestampValue(value, (TimestampType) type);
    }

    throw new UnsupportedOperationException("Unsupported type: " + type);
  }

  protected Row convertStructValue(Object value, StructType schema) {
    if (value instanceof Map) {
      return convertToStruct((Map<?, ?>) value, schema);
    } else if (value instanceof Struct) {
      return convertToStruct((Struct) value, schema);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  /**
   * This method will be called for records when there is no record schema. Also, when there is no
   * schema, we infer that map values are struct types. This method might also be called if the
   * field value is a map but the Iceberg type is a struct. This can happen if the Iceberg table
   * schema is not managed by the sink, i.e. created manually.
   */
  private Row convertToStruct(Map<?, ?> map, StructType schema) {
    HashMap<Integer, Object> valueMap = new HashMap<>();
    map.forEach(
        (recordFieldNameObj, recordFieldValue) -> {
          String recordFieldName = recordFieldNameObj.toString();
          StructField field = schema.get(recordFieldName);
          int ordinal = schema.indexOf(recordFieldName);

          Object value = convertValue(recordFieldValue, field.getDataType());
          valueMap.put(ordinal, value);
        });

    return new GenericRow(schema, valueMap);
  }

  /** This method will be called for records and struct values when there is a record schema. */
  private Row convertToStruct(Struct struct, StructType schema) {
    HashMap<Integer, Object> valueMap = new HashMap<>();
    struct
        .schema()
        .fields()
        .forEach(
            recordField -> {
              String recordFieldName = recordField.name();
              StructField field = schema.get(recordFieldName);
              int ordinal = schema.indexOf(recordFieldName);

              Object value = convertValue(struct.get(recordField), field.getDataType());
              valueMap.put(ordinal, value);
            });
    return new GenericRow(schema, valueMap);
  }

  protected List<Object> convertListValue(Object value, ArrayType type) {
    Preconditions.checkArgument(value instanceof List);
    List<?> list = (List<?>) value;
    return list.stream()
        .map(
            element -> {
              DataType el = type.getElementType();
              return convertValue(element, el);
            })
        .collect(Collectors.toList());
  }

  protected Map<Object, Object> convertMapValue(Object value, MapType type) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = Maps.newHashMap();
    map.forEach(
        (k, v) -> {
          DataType keyType = type.getKeyType();
          DataType valueType = type.getValueType();
          result.put(convertValue(k, keyType), convertValue(v, valueType));
        });
    return result;
  }

  protected int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  protected long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  protected float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  protected double convertDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  protected BigDecimal convertDecimal(Object value, DecimalType type) {
    BigDecimal bigDecimal;
    if (value instanceof BigDecimal) {
      bigDecimal = (BigDecimal) value;
    } else if (value instanceof Number) {
      Number num = (Number) value;
      Double dbl = num.doubleValue();
      if (dbl.equals(Math.floor(dbl))) {
        bigDecimal = BigDecimal.valueOf(num.longValue());
      } else {
        bigDecimal = BigDecimal.valueOf(dbl);
      }
    } else if (value instanceof String) {
      bigDecimal = new BigDecimal((String) value);
    } else {
      throw new IllegalArgumentException(
          "Cannot convert to BigDecimal: " + value.getClass().getName());
    }
    return bigDecimal.setScale(type.getScale(), RoundingMode.HALF_UP);
  }

  protected boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  protected String convertString(Object value) {
    try {
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof Number || value instanceof Boolean) {
        return value.toString();
      } else if (value instanceof Map || value instanceof List) {
        return MAPPER.writeValueAsString(value);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalDate convertDateValue(Object value) {
    throw new ConnectException("Cannot convert date: " + value);
  }

  protected Temporal convertTimestampValue(Object value, TimestampType type) {
    throw new ConnectException("Cannot convert timestamp: " + value);
  }
}
