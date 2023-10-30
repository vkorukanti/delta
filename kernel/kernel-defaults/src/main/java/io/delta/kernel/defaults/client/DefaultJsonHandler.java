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
package io.delta.kernel.defaults.client;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.storage.LocalLogStore;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.defaults.internal.types.DataTypeParser;

/**
 * Default implementation of {@link JsonHandler} based on Hadoop APIs.
 */
public class DefaultJsonHandler implements JsonHandler {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectReader defaultObjectReader = mapper.reader();
    // by default BigDecimals are truncated and read as floats
    private static final ObjectReader objectReaderReadBigDecimals = mapper
        .reader(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final Configuration hadoopConf;
    private final int maxBatchSize;

    public DefaultJsonHandler(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        this.maxBatchSize =
            hadoopConf.getInt("delta.kernel.default.json.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid JSON reader batch size: " + maxBatchSize);
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema,
            Optional<ColumnVector> selectionVector) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < jsonStringVector.getSize(); i++) {
            boolean isSelected = !selectionVector.isPresent() ||
                (!selectionVector.get().isNullAt(i) && selectionVector.get().getBoolean(i));
            if (isSelected && !jsonStringVector.isNullAt(i)) {
                rows.add(
                    parseJson(jsonStringVector.getString(i), outputSchema));
            } else {
                rows.add(null);
            }
        }
        return new DefaultRowBasedColumnarBatch(outputSchema, rows);
    }

    @Override
    public StructType deserializeStructType(String structTypeJson) {
        try {
            // We don't expect Java BigDecimal anywhere in a Delta schema so we use the default
            // JSON reader
            return DataTypeParser.parseSchema(defaultObjectReader.readTree(structTypeJson));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(
                String.format("Could not parse JSON: %s", structTypeJson), ex);
        }
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(
        CloseableIterator<FileStatus> scanFileIter,
        StructType physicalSchema,
        Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private FileStatus currentFile;
            private BufferedReader currentFileReader;
            private String nextLine;

            @Override
            public void close()
                throws IOException {
                Utils.closeCloseables(currentFileReader, scanFileIter);
            }

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null ||
                        (nextLine = currentFileReader.readLine()) == null) {

                        tryOpenNextFile();
                        if (currentFileReader != null) {
                            nextLine = currentFileReader.readLine();
                        }
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                return nextLine != null;
            }

            @Override
            public ColumnarBatch next() {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }

                List<Row> rows = new ArrayList<>();
                int currentBatchSize = 0;
                do {
                    // hasNext already reads the next one and keeps it in member variable `nextLine`
                    rows.add(parseJson(nextLine, physicalSchema));
                    nextLine = null;
                    currentBatchSize++;
                }
                while (currentBatchSize < maxBatchSize && hasNext());

                return new DefaultRowBasedColumnarBatch(physicalSchema, rows);
            }

            private void tryOpenNextFile()
                throws IOException {
                Utils.closeCloseables(currentFileReader); // close the current opened file
                currentFileReader = null;

                if (scanFileIter.hasNext()) {
                    currentFile = scanFileIter.next();
                    Path filePath = new Path(currentFile.getPath());
                    FileSystem fs = filePath.getFileSystem(hadoopConf);
                    FSDataInputStream stream = null;
                    try {
                        stream = fs.open(filePath);
                        currentFileReader = new BufferedReader(
                            new InputStreamReader(stream, StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
                        throw e;
                    }
                }
            }
        };
    }

    @Override
    public void writeJsonFileAtomically(String filePath, CloseableIterator<Row> data)
        throws IOException {
        // TODO: We need an API at the `delta-storage` or in `kernel-defaults` to create
        // LogStore based on scheme in the file path. Similar to the standalone `LogStoreProvider`.
        LogStore logStore = new LocalLogStore(hadoopConf);
        Path path = new Path(filePath);
        path = logStore.resolvePathOnPhysicalStorage(path, hadoopConf);
        try {
            logStore.write(
                path,
                new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return data.hasNext();
                    }

                    @Override
                    public String next() {
                        return serializeAsJson(data.next());
                    }
                },
                false /* overwrite */,
                hadoopConf);
        } finally {
            Utils.closeCloseables(data);
        }
    }

    private String serializeAsJson(Row row) {
        try {
            Map<String, Object> map = convertToMap(row);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not serialize JSON: %s", row), ex);
        }
    }

    private Row parseJson(String json, StructType readSchema) {
        try {
            final JsonNode jsonNode = objectReaderReadBigDecimals.readTree(json);
            return new DefaultJsonRow((ObjectNode) jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }

    private Map<String, Object> convertToMap(Row row) {
        Map<String, Object> map = new HashMap<>();

        StructType schema = row.getSchema();
        for (int columnOrdinal = 0; columnOrdinal < schema.length(); columnOrdinal++) {
            StructField field = schema.at(columnOrdinal);
            DataType dataType = field.getDataType();
            if (dataType instanceof MapType) {
                MapType mapType = (MapType) dataType;
                if (!(mapType.getKeyType() instanceof StringType)) {
                    throw new IllegalArgumentException("Only string key type is supported in " +
                            "map type for serialization to JSON");
                }
            }
            Object value =
                VectorUtils.getValueAsObject(row, columnOrdinal, field.getDataType());
            if (value instanceof Row) {
                value = convertToMap((Row) value);
            }

            map.put(field.getName(), value);
        }

        return map;
    }
}
