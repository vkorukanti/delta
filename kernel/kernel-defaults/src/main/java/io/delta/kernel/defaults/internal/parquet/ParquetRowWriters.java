/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils.MAX_BYTES_PER_PRECISION;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * Parquet row writers for writing {@link Row} to Parquet file using the {@link RecordConsumer}
 * interface.
 */
public class ParquetRowWriters {
  private ParquetRowWriters() {}

  /**
   * Base class for column writers. Handles the common stuff such as null check, start/stop of field
   * and delegating the actual writing of non-null values to the subclass.
   */
  public abstract static class ColumnWriter {
    protected final String colName;
    protected final int fieldIndex;

    ColumnWriter(String colName, int fieldIndex) {
      this.colName = colName;
      this.fieldIndex = fieldIndex;
    }

    public void writeColumnValue(RecordConsumer recordConsumer, Row row) {
      if (row.isNullAt(fieldIndex)) {
        return;
      }
      recordConsumer.startField(colName, fieldIndex);
      writeNonNullColumnValue(recordConsumer, row);
      recordConsumer.endField(colName, fieldIndex);
    }

    /**
     * Each specific column writer for data type, will implement to call appropriate methods on the
     * {@link RecordConsumer} to write the non-null value.
     */
    abstract void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row);
  }

  /**
   * Create column writers for the given row schema.
   *
   * @param rowSchema schema of the row
   * @return an array of column writers to write the row to Parquet file
   */
  public static ColumnWriter[] createColumnWriters(StructType rowSchema) {
    requireNonNull(rowSchema, "rowSchema is null");
    return createColumnWritersHelper(rowSchema);
  }

  private static ColumnWriter[] createColumnWritersHelper(StructType schema) {
    int numCols = schema.length();

    ColumnWriter[] columnWriters = new ColumnWriter[numCols];
    for (int fieldIndex = 0; fieldIndex < numCols; fieldIndex++) {
      String colName = schema.at(fieldIndex).getName();
      columnWriters[fieldIndex] =
          createColumnWriter(colName, fieldIndex, schema.at(fieldIndex).getDataType());
    }
    return columnWriters;
  }

  private static ColumnWriter createColumnWriter(
      String colName, int fieldIndex, DataType dataType) {

    if (dataType instanceof BooleanType) {
      return new BooleanWriter(colName, fieldIndex);
    } else if (dataType instanceof ByteType) {
      return new ByteWriter(colName, fieldIndex);
    } else if (dataType instanceof ShortType) {
      return new ShortWriter(colName, fieldIndex);
    } else if (dataType instanceof IntegerType) {
      return new IntWriter(colName, fieldIndex);
    } else if (dataType instanceof LongType) {
      return new LongWriter(colName, fieldIndex);
    } else if (dataType instanceof FloatType) {
      return new FloatWriter(colName, fieldIndex);
    } else if (dataType instanceof DoubleType) {
      return new DoubleWriter(colName, fieldIndex);
    } else if (dataType instanceof StringType) {
      return new StringWriter(colName, fieldIndex);
    } else if (dataType instanceof BinaryType) {
      return new BinaryWriter(colName, fieldIndex);
    } else if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      int precision = decimalType.getPrecision();
      int scale = decimalType.getScale();
      if (precision <= ParquetSchemaUtils.DECIMAL_MAX_DIGITS_IN_INT) {
        return new DecimalIntWriter(colName, fieldIndex, scale);
      } else if (precision <= ParquetSchemaUtils.DECIMAL_MAX_DIGITS_IN_LONG) {
        return new DecimalLongWriter(colName, fieldIndex, scale);
      }
      // TODO: Need to support legacy mode where all decimals are written as binary
      return new DecimalFixedBinaryWriter(colName, fieldIndex, precision, scale);
    } else if (dataType instanceof DateType) {
      return new DateWriter(colName, fieldIndex);
    } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
      // for both get the input as long type from column vector and write to file as INT64
      return new TimestampWriter(colName, fieldIndex);
    } else if (dataType instanceof ArrayType) {
      return new ArrayWriter(colName, fieldIndex);
    } else if (dataType instanceof MapType) {
      return new MapWriter(colName, fieldIndex);
    } else if (dataType instanceof StructType) {
      return new StructWriter(colName, fieldIndex, (StructType) dataType);
    }

    throw new IllegalArgumentException("Unsupported column vector type: " + dataType);
  }

  static class BooleanWriter extends ColumnWriter {
    BooleanWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addBoolean(row.getBoolean(fieldIndex));
    }
  }

  static class ByteWriter extends ColumnWriter {
    ByteWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addInteger(row.getByte(fieldIndex));
    }
  }

  static class ShortWriter extends ColumnWriter {
    ShortWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addInteger(row.getShort(fieldIndex));
    }
  }

  static class IntWriter extends ColumnWriter {
    IntWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addInteger(row.getInt(fieldIndex));
    }
  }

  static class LongWriter extends ColumnWriter {
    LongWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addLong(row.getLong(fieldIndex));
    }
  }

  static class FloatWriter extends ColumnWriter {
    FloatWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addFloat(row.getFloat(fieldIndex));
    }
  }

  static class DoubleWriter extends ColumnWriter {
    DoubleWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.addDouble(row.getDouble(fieldIndex));
    }
  }

  static class DecimalIntWriter extends ColumnWriter {
    private final int scale;

    DecimalIntWriter(String name, int fieldIndex, int scale) {
      super(name, fieldIndex);
      this.scale = scale;
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      BigDecimal decimal = row.getDecimal(fieldIndex).movePointRight(scale);
      recordConsumer.addInteger(decimal.intValue());
    }
  }

  static class DecimalLongWriter extends ColumnWriter {
    private final int scale;

    DecimalLongWriter(String name, int fieldIndex, int scale) {
      super(name, fieldIndex);
      this.scale = scale;
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      BigDecimal decimal = row.getDecimal(fieldIndex).movePointRight(scale);
      recordConsumer.addLong(decimal.longValue());
    }
  }

  static class DecimalFixedBinaryWriter extends ColumnWriter {
    private final int numBytes;
    private final byte[] reusedBuffer;

    DecimalFixedBinaryWriter(String name, int fieldIndex, int precision, int scale) {
      super(name, fieldIndex);
      this.numBytes = MAX_BYTES_PER_PRECISION.get(precision);
      this.reusedBuffer = new byte[numBytes];
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      byte[] bytes = row.getDecimal(fieldIndex).unscaledValue().toByteArray();

      Binary binary;
      if (bytes.length == numBytes) {
        // If the length of the underlying byte array of the unscaled `BigInteger`
        // happens to be `numBytes`, just reuse it, so that we don't bother
        // copying it to `reusedBuffer`.
        binary = Binary.fromReusedByteArray(bytes);
      } else {
        // Otherwise, the length must be less than `numBytes`.  In this case we copy
        // contents of the underlying bytes with padding sign bytes to `decimalBuffer`
        // to form the result fixed-length byte array.
        byte signByte = (bytes[0] < 0) ? (byte) -1 : (byte) 0;
        Arrays.fill(reusedBuffer, 0, numBytes - bytes.length, signByte);
        System.arraycopy(bytes, 0, reusedBuffer, numBytes - bytes.length, bytes.length);
        binary = Binary.fromReusedByteArray(reusedBuffer);
      }

      recordConsumer.addBinary(binary);
    }
  }

  static class DateWriter extends ColumnWriter {
    DateWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      // TODO: Spark has various handling mode for DateType, need to check if it is needed
      // for Delta Kernel.
      recordConsumer.addInteger(row.getInt(fieldIndex)); // dates are stores as epoch days
    }
  }

  /** Writer for both timestamp and timestamp with time zone. */
  static class TimestampWriter extends ColumnWriter {
    TimestampWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      long microsSinceEpochUTC = row.getLong(fieldIndex);
      recordConsumer.addLong(microsSinceEpochUTC);
    }
  }

  static class StringWriter extends ColumnWriter {
    StringWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      Binary binary =
          Binary.fromConstantByteArray(row.getString(fieldIndex).getBytes(StandardCharsets.UTF_8));
      recordConsumer.addBinary(binary);
    }
  }

  static class BinaryWriter extends ColumnWriter {
    BinaryWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      Binary binary = Binary.fromConstantByteArray(row.getBinary(fieldIndex));
      recordConsumer.addBinary(binary);
    }
  }

  static class ArrayWriter extends ColumnWriter {
    ArrayWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      // Write as 3-level representation. Later on, depending upon the config,
      // we can write either as 2-level or 3-level representation.
      recordConsumer.startGroup();
      ArrayValue arrayValue = row.getArray(fieldIndex);
      if (arrayValue.getSize() > 0) {

        recordConsumer.startField("list", 0 /* fieldIndex */);
        ColumnVector elementVector = arrayValue.getElements();
        ParquetColumnWriters.ColumnWriter elementWriter =
            ParquetColumnWriters.createColumnWriter("element", 0 /* fieldIndex */, elementVector);
        for (int i = 0; i < arrayValue.getSize(); i++) {
          recordConsumer.startGroup();
          if (!elementVector.isNullAt(i)) {
            elementWriter.writeRowValue(recordConsumer, i);
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField("list", 0 /* fieldIndex */);
      }
      recordConsumer.endGroup();
    }
  }

  static class MapWriter extends ColumnWriter {
    MapWriter(String name, int fieldIndex) {
      super(name, fieldIndex);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      // Write as 3-level representation. Later on, depending upon the config,
      // we can write either as 2-level or 3-level representation.
      recordConsumer.startGroup();

      MapValue mapValue = row.getMap(fieldIndex);
      if (mapValue.getSize() > 0) {
        recordConsumer.startField("key_value", 0 /* fieldIndex */);

        // Use the fieldIndex as zero. Once we support Uniform compatible Parquet files,
        // the field index will come from the Delta schema.
        ColumnVector keyVector = mapValue.getKeys();
        ParquetColumnWriters.ColumnWriter keyWriter =
            ParquetColumnWriters.createColumnWriter("key", 0 /* fieldIndex */, keyVector);
        ColumnVector valueVector = mapValue.getValues();
        ParquetColumnWriters.ColumnWriter valueWriter =
            ParquetColumnWriters.createColumnWriter("value", 1 /* fieldIndex */, valueVector);

        for (int i = 0; i < mapValue.getSize(); i++) {
          recordConsumer.startGroup();
          keyWriter.writeRowValue(recordConsumer, i);
          if (!valueVector.isNullAt(i)) {
            valueWriter.writeRowValue(recordConsumer, i);
          }
          recordConsumer.endGroup();
        }

        recordConsumer.endField("key_value", 0 /* fieldIndex */);
      }
      recordConsumer.endGroup();
    }
  }

  static class StructWriter extends ColumnWriter {
    private final ColumnWriter[] fieldWriters;

    StructWriter(String name, int fieldIndex, StructType schema) {
      super(name, fieldIndex);
      fieldWriters = createColumnWritersHelper(schema);
    }

    @Override
    void writeNonNullColumnValue(RecordConsumer recordConsumer, Row row) {
      recordConsumer.startGroup();
      Row structRow = row.getStruct(fieldIndex);
      for (ColumnWriter fieldWriter : fieldWriters) {
        fieldWriter.writeColumnValue(recordConsumer, structRow);
      }
      recordConsumer.endGroup();
    }
  }
}
