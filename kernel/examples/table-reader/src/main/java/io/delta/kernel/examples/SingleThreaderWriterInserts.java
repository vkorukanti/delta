package io.delta.kernel.examples;

import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.Utils;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.defaults.client.DefaultTableClient;

import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;

public class SingleThreaderWriterInserts {

    private static final TableClient tableClient =
            DefaultTableClient.create(new Configuration());
    public static void main(String[] args)
            throws Exception {

        String path = "/tmp/test-delta-inserts";

        Table table = Table.forPath(tableClient, path);

        StructType schema = new StructType()
                .add("id", IntegerType.INTEGER)
                .add("part1", IntegerType.INTEGER);

        TransactionBuilder txnBuilder = table.createTransactionBuilder(
                tableClient,
                "Kernel/Examples", /* engine info */
                "INSERT" /* operation */);

        txnBuilder = txnBuilder
                .withSchema(tableClient, schema)
                .withPartitionColumns(tableClient, Collections.singleton("part1"));

        Transaction txn = txnBuilder.build(tableClient);

        Row txnState = txn.getState(tableClient);

        // Create the data for partition part1 = 100
        CloseableIterator<FilteredColumnarBatch> dataIter =
                Utils.singletonCloseableIterator(dataBatch(
                        schema,
                        intVector(Arrays.asList(1, 2, 3, 4, 5)),
                        intSingleValueVector(5, 100)));

        Map<String, Literal> partitionValues = new HashMap<String, Literal>() {
            {
                this.put("part1", Literal.ofInt(100));
            }
        });

        // Transform the data to physical format that go directly into the data files
        CloseableIterator<FilteredColumnarBatch> dataIterTransformed =
                Transaction.transformLogicalData(tableClient, txnState, dataIter, partitionValues);

        WriteContext writeContext = Transaction.getWriteContext(
                tableClient,
                txnState,
                partitionValues);

        // Write the physical data into Parquet files
        CloseableIterator<Row> fileStatusIter = tableClient
                .getParquetHandler()
                .writeParquetFiles(
                        writeContext.getTargetDirectory(),
                        dataIterTransformed,
                        writeContext.getTargetFileSizeInBytes(),
                        writeContext.getStatisticsSchema());

        // Convert the staged file actions into a delta log actions
        CloseableIterator<Row> stagedActions = Transaction
                .stageAppendOnlyData(tableClient, txnState, fileStatusIter, partitionValues);


        // Save the actions from iterator in a list, so that we can access the actions
        // multiple times in case of the conflict resolution
        List<Row> savedActions = new ArrayList<>();
        stagedActions.forEachRemaining(savedActions::add);

        Optional<TransactionCommitStatus> lastCommitStatus = Optional.empty();
        // In a loop keep retrying until the commit is successful or max retries reached.
        // Or a non-retryable conflict occurs.
        for (int i = 0; i < 3; i++) {
            TransactionCommitStatus commitStatus;
            try {
                commitStatus = txn.commit(
                        tableClient,
                        toCloseableIterator(stagedActions),
                        lastCommitStatus
                );
            } catch (TransactionNonRetryableConflictException e) {
                throw e;
            }

            if (commitStatus.isCommitted()) {
                break;
            }

            lastCommitStatus = Optional.of(commitStatus);
        }

        if (lastCommitStatus.get().canCheckpoint()) {
            table.checkpoint(tableClient, lastCommitStatus.get().getVersion());
        }
    }

    static FilteredColumnarBatch dataBatch(
            StructType schema,
            ColumnVector vector1,
            ColumnVector vector2) {
        ColumnarBatch batch = new DefaultColumnarBatch(
                vector1.getSize(),
                schema,
                Arrays.asList(vector1, vector2).toArray(new ColumnVector[2]));

        return new FilteredColumnarBatch(
                batch,
                Optional.empty());
    }

    static ColumnVector intVector(List<Integer> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isNullAt(int rowId) {
                return false;
            }

            @Override
            public int getInt(int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector intSingleValueVector(int size, int value) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
                return size;
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isNullAt(int rowId) {
                return false;
            }

            @Override
            public int getInt(int rowId) {
                return value;
            }
        };
    }
}
