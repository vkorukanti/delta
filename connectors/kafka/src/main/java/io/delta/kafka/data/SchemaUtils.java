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

import io.delta.kafka.DeltaSinkConfig;
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
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

class SchemaUtils {
  static DataType toDeltaType(Schema valueSchema, DeltaSinkConfig config) {
    return new SchemaGenerator(config).toDeltaType(valueSchema);
  }

  static DataType inferDeltaType(Object value, DeltaSinkConfig config) {
    return new SchemaGenerator(config).inferDeltaType(value);
  }

  static class SchemaGenerator {

    private int fieldId = 1;
    private final DeltaSinkConfig config;

    SchemaGenerator(DeltaSinkConfig config) {
      this.config = config;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    DataType toDeltaType(Schema valueSchema) {
      switch (valueSchema.type()) {
        case BOOLEAN:
          return BooleanType.BOOLEAN;
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(valueSchema.name())) {
            int scale = Integer.parseInt(valueSchema.parameters().get(Decimal.SCALE_FIELD));
            return new DecimalType(38, scale);
          }
          return BinaryType.BINARY;
        case INT8:
        case INT16:
          return IntegerType.INTEGER;
        case INT32:
          if (Date.LOGICAL_NAME.equals(valueSchema.name())) {
            return DateType.DATE;
          } else if (Time.LOGICAL_NAME.equals(valueSchema.name())) {
            throw new UnsupportedOperationException("Time type is not supported");
          }
          return IntegerType.INTEGER;
        case INT64:
          if (Timestamp.LOGICAL_NAME.equals(valueSchema.name())) {
            return TimestampType.TIMESTAMP;
          }
          return LongType.LONG;
        case FLOAT32:
          return FloatType.FLOAT;
        case FLOAT64:
          return DoubleType.DOUBLE;
        case ARRAY:
          DataType elementType = toDeltaType(valueSchema.valueSchema());
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return new ArrayType(elementType, true);
          } else {
            return new ArrayType(elementType, false);
          }
        case MAP:
          DataType keyType = toDeltaType(valueSchema.keySchema());
          DataType valueType = toDeltaType(valueSchema.valueSchema());
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return new MapType(keyType, valueType, true);
          } else {
            return new MapType(keyType, valueType, false);
          }
        case STRUCT:
          List<StructField> structFields =
              valueSchema.fields().stream()
                  .map(
                      field ->
                          new StructField(
                              field.name(),
                              toDeltaType(field.schema()),
                              config.schemaForceOptional() || field.schema().isOptional()))
                  .collect(Collectors.toList());
          return new StructType(structFields);
        case STRING:
          return StringType.STRING;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + valueSchema.type());
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    DataType inferDeltaType(Object value) {
      if (value == null) {
        return null;
      } else if (value instanceof String) {
        return StringType.STRING;
      } else if (value instanceof Boolean) {
        return BooleanType.BOOLEAN;
      } else if (value instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) value;
        return new DecimalType(bigDecimal.precision(), bigDecimal.scale());
      } else if (value instanceof Integer || value instanceof Long) {
        return LongType.LONG;
      } else if (value instanceof Float || value instanceof Double) {
        return DoubleType.DOUBLE;
      } else if (value instanceof LocalDate) {
        return DateType.DATE;
      } else if (value instanceof LocalTime) {
        throw new UnsupportedOperationException("Time type is not supported");
      } else if (value instanceof java.util.Date || value instanceof OffsetDateTime) {
        return TimestampType.TIMESTAMP;
      } else if (value instanceof LocalDateTime) {
        return TimestampNTZType.TIMESTAMP_NTZ;
      } else if (value instanceof List) {
        List<?> list = (List<?>) value;
        if (list.isEmpty()) {
          return null;
        }
        DataType elementType = inferDeltaType(list.get(0));
        return elementType == null ? null : new ArrayType(elementType, false);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        List<StructField> structFields =
            map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(
                    entry -> {
                      DataType valueType = inferDeltaType(entry.getValue());
                      return valueType == null
                          ? null
                          : new StructField(entry.getKey().toString(), valueType, true);
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (structFields.isEmpty()) {
          return null;
        }
        return new StructType(structFields);
      } else {
        return null;
      }
    }
  }

  private SchemaUtils() {}
}
