/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.TransactionImpl
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.{Table, Transaction, TransactionBuilder, TransactionCommitStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

class DeltaTableWritesSuite extends AnyFunSuite with TestUtils {

  /** Test table schemas */
  val testSchema = new StructType().add("id", INTEGER)
  val testPartitionSchema = new StructType()
    .add("id", INTEGER)
    .add("part1", INTEGER) // partition column
    .add("part2", INTEGER) // partition column

  val dataBatch1 = testBatch(200)
  val dataBatch2 = testBatch(400)

  val dataPartitionBatch1 = testPartitionBatch(size = 236, part1 = 1, part2 = 2)
  val dataPartitionBatch2 = testPartitionBatch(size = 876, part1 = 4, part2 = 5)

  test("insert into table - table created from scratch") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val table = Table.forPath(defaultTableClient, tblPath)
      val newVersion1 = writeData(isNewTable = true, table, dataBatch1, dataBatch2)
      assert(newVersion1.isCommitted)
      assert(newVersion1.getVersion == 0)
      val expectedAnswer = toTestRows(dataBatch1) ++ toTestRows(dataBatch2)

      checkTable(path = tblPath, expectedAnswer)
    }
  }

  test("insert into table - already existing table") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val table = Table.forPath(defaultTableClient, tblPath)

      {
        val newVersion1 = writeData(isNewTable = true, table, dataBatch1, dataBatch2)
        assert(newVersion1.isCommitted)
        assert(newVersion1.getVersion == 0)

        val expectedAnswer1 = toTestRows(dataBatch1) ++ toTestRows(dataBatch2)
        checkTable(tblPath, expectedAnswer1)
      }
      {
        val newVersion2 = writeData(isNewTable = false, table, dataBatch2)
        assert(newVersion2.isCommitted)
        assert(newVersion2.getVersion == 1)

        val expectedAnswer2 = toTestRows(dataBatch1) ++ toTestRows(dataBatch2) ++ // version 0
          toTestRows(dataBatch2) // version 1
        checkTable(tblPath, expectedAnswer2)
      }
    }
  }

  test("insert into table - fails when creating table without schema") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath
      val table = Table.forPath(defaultTableClient, tblPath)

      val ex = intercept[IllegalArgumentException] {
        createTxnBuilder(table)
          .build(defaultTableClient)
      }
      assert(ex.getMessage.contains("Table doesn't exist yet. Must provide a new schema"))
    }
  }

  test("insert into table - fails when committing the same txn twice") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath
      val table = Table.forPath(defaultTableClient, tblPath)

      val txn = createTxnBuilder(table)
        .withSchema(defaultTableClient, testSchema)
        .build(defaultTableClient)

      val txnState = txn.getState(defaultTableClient)

      val closebleIterBatches =
        toCloseableIterator(Seq(dataBatch1).toIterator.asJava)
      val stagedFiles = stageData(txnState, Map.empty, closebleIterBatches)

      val newVersion = txn.commit(defaultTableClient, stagedFiles, Optional.empty())
      assert(newVersion.isCommitted)
      assert(newVersion.getVersion == 0)

      // try to commit the same transaction and expect failure
      val ex = intercept[IllegalStateException] {
        txn.commit(defaultTableClient, stagedFiles, Optional.empty())
      }
      assert(ex.getMessage.contains("Transaction is already committed. Create a new transaction."))
    }
  }

  test("insert into partitioned table - table created from scratch") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val table = Table.forPath(defaultTableClient, tblPath)
      val newVersion1 = writeDataWithPartitions(
        isNewTable = true,
        table,
        Map("part1" -> Literal.ofInt(1), "part2" -> Literal.ofInt(2)),
        dataPartitionBatch1)
      assert(newVersion1.isCommitted)
      assert(newVersion1.getVersion == 0)

      val expectedAnswer =
        toTestRows(dataPartitionBatch1)

      checkTable(path = tblPath, expectedAnswer)
    }
  }

  test("insert into partitioned table - already existing table") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val table = Table.forPath(defaultTableClient, tblPath)

      {
        val newVersion1 = writeDataWithPartitions(
          isNewTable = true,
          table,
          Map("part1" -> Literal.ofInt(1), "part2" -> Literal.ofInt(2)),
          dataPartitionBatch1)
        assert(newVersion1.isCommitted)
        assert(newVersion1.getVersion == 0)

        val expectedAnswer1 = toTestRows(dataPartitionBatch1)
        checkTable(tblPath, expectedAnswer1)
      }
      {
        val newVersion2 = writeDataWithPartitions(
          isNewTable = false,
          table,
          Map("part1" -> Literal.ofInt(4), "part2" -> Literal.ofInt(5)),
          dataPartitionBatch2)
        assert(newVersion2.isCommitted)
        assert(newVersion2.getVersion == 1)

        val expectedAnswer2 = {
          // version 0
          toTestRows(dataPartitionBatch1)
        } ++ {
          // version 1
          toTestRows(dataPartitionBatch2)
        }
        checkTable(tblPath, expectedAnswer2)
      }
    }
  }

  test("insert into table - idempotent writes") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val table = Table.forPath(defaultTableClient, tblPath)

      def addDataWithTxnId(appId: String, txnVersion: Long, expCommitVersion: Long): Unit = {
        val txn = createTxnBuilder(table)
          .withSchema(defaultTableClient, testSchema)
          .withTransactionId(defaultTableClient, appId, txnVersion)
          .build(defaultTableClient)

        val txnState = txn.getState(defaultTableClient)

        val closebleIterBatches =
          toCloseableIterator(Seq(dataBatch1, dataBatch2).toIterator.asJava)
        val stagedFiles = stageData(txnState, Map.empty, closebleIterBatches)

        val newVersion = txn.commit(defaultTableClient, stagedFiles, Optional.empty())
        assertCommit(expCommitVersion, newVersion)
      }

      def testData(): Seq[TestRow] = toTestRows(dataBatch1) ++ toTestRows(dataBatch2)

      {
        // Create a transaction with id (txnAppId1, 0) and commit it
        addDataWithTxnId(appId = "txnAppId1", txnVersion = 0, expCommitVersion = 0)
        val expectedAnswer = testData() /* v0 */
        checkTable(path = tblPath, expectedAnswer)
      }
      {
        // Try to create a transaction with id (txnAppId1, 0) and commit it - should be valid
        addDataWithTxnId("txnAppId1", txnVersion = 1, expCommitVersion = 1)
        val expectedAnswer = testData() /* v0 */ ++ testData() /* v1 */
        checkTable(path = tblPath, expectedAnswer)
      }
      {
        // Try to create a transaction with id (txnAppId1, 1) and try to commit it
        // Should fail the it is already committed above.
        val ex = intercept[IllegalArgumentException] {
          addDataWithTxnId("txnAppId1", txnVersion = 1, expCommitVersion = 2)
        }
        assert(ex.getMessage.contains(
          "Transaction with identifier is already committed. " +
            "ApplicationId: txnAppId1, Transaction version: 1. " +
            "Last committed transaction version: 1"))
      }
      {
        // Try to create a transaction with id (txnAppId2, 1) and commit it
        // Should be successful as the transaction app id is different
        addDataWithTxnId("txnAppId2", txnVersion = 1, expCommitVersion = 2)

        val expectedAnswer = testData() /* v0 */ ++ testData() /* v1 */ ++ testData() /* v2 */
        checkTable(path = tblPath, expectedAnswer)
      }
      {
        // Try to create a transaction with id (txnAppId2, 0) and commit it
        // Should fail as the transaction app id is same but the version is less than the committed
        val ex = intercept[IllegalArgumentException] {
          addDataWithTxnId("txnAppId2", txnVersion = 0, expCommitVersion = 3)
        }
        assert(ex.getMessage.contains(
          "Transaction with identifier is already committed. " +
            "ApplicationId: txnAppId2, Transaction version: 0. " +
            "Last committed transaction version: 1"))
      }
      {
        // TODO: Add a test case where there are concurrent transactions with same app id
        // and only one of them succeeds.
      }
    }
  }

  def assertCommit(expVersion: Long, commitStatus: TransactionCommitStatus): Unit = {
    assert(commitStatus.isCommitted)
    assert(commitStatus.getVersion == expVersion)
  }

  def writeData(isNewTable: Boolean, table: Table, batches: FilteredColumnarBatch*)
  : TransactionCommitStatus = {
    writeDataWithPartitions(isNewTable, table, Map.empty, batches: _*)
  }

  def writeDataWithPartitions(
      isNewTable: Boolean,
      table: Table,
      partitionValues: Map[String, Literal],
      batches: FilteredColumnarBatch*): TransactionCommitStatus = {
    var txn1Builder = createTxnBuilder(table)
    if (isNewTable) {
      val (schema, partitionCols: Set[String]) =
        if (partitionValues.isEmpty) {
          (testSchema, Set.empty)
        } else {
          (testPartitionSchema, Set("part1", "part2"))
        }
      txn1Builder = txn1Builder
        .withSchema(defaultTableClient, schema)

      if (isNewTable) {
        txn1Builder = txn1Builder
          .withPartitionColumns(defaultTableClient, partitionCols.asJava)
      }
    }
    val txn1 = txn1Builder
      .build(defaultTableClient)

    val txn1State = txn1.getState(defaultTableClient)

    val closebleIterBatches =
      toCloseableIterator(batches.toIterator.asJava)
    val stagedFiles1 = stageData(txn1State, partitionValues, closebleIterBatches)

    txn1.commit(defaultTableClient, stagedFiles1, Optional.empty())
  }

  def createTxnBuilder(table: Table): TransactionBuilder = {
    table.createTransactionBuilder(
      defaultTableClient,
      "Delta Kernel 3.1.0", // engine info
      "INSERT"
    )
  }

  def testBatch(size: Integer): FilteredColumnarBatch = {
    val intVector = testColumnVector(size, INTEGER)
    val batch1: ColumnarBatch =
      new DefaultColumnarBatch(intVector.getSize, testSchema, Seq(intVector).toArray)
    new FilteredColumnarBatch(batch1, Optional.empty())
  }

  def testPartitionBatch(size: Integer, part1: Int, part2: Int): FilteredColumnarBatch = {
    val intVector = testColumnVector(size, INTEGER)
    val intPart1Vector = testSingleValueVector(INTEGER, size, part1)
    val intPart2Vector = testSingleValueVector(INTEGER, size, part2)
    val batch1: ColumnarBatch =
      new DefaultColumnarBatch(
        intVector.getSize,
        testPartitionSchema,
        Seq(intVector, intPart1Vector, intPart2Vector).toArray)
    new FilteredColumnarBatch(batch1, Optional.empty())
  }

  def toTestRows(batch: FilteredColumnarBatch): Seq[TestRow] = {
    val selVector = batch.getSelectionVector()
    (0 until batch.getData.getSize).map { idx =>
      val isRowSelected = !selVector.isPresent ||
        (!selVector.get().isNullAt(idx) && selVector.get().getBoolean(idx))
      if (!isRowSelected) {
        TestRow(Seq(null): _*)
      } else {
        val values = (0 until batch.getData.getSchema.length()).map { colIdx =>
          val columnVector = batch.getData.getColumnVector(colIdx)
          assert(columnVector.getDataType == INTEGER)
          if (columnVector.isNullAt(idx)) {
            null
          } else {
            columnVector.getInt(idx)
          }
        }.toSeq
        TestRow(values: _*)
      }
    }.toSeq
  }

  def stageData(
      state: Row,
      partitionValues: Map[String, Literal],
      data: CloseableIterator[FilteredColumnarBatch])
  : CloseableIterator[Row] = {
    val physicalDataIter =
      Transaction.transformLogicalData(defaultTableClient, state, data, partitionValues.asJava)

    val writeContext = Transaction.getWriteContext(
      defaultTableClient, state, partitionValues.asJava)

    val writeResultIter = defaultTableClient
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getTargetFileSizeInBytes,
        writeContext.getStatisticsSchema)

    Transaction.stageAppendOnlyData(defaultTableClient, state, writeResultIter, writeContext)
  }
}
