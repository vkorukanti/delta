/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.Table
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.internal.coordinatedcommits.CommitCoordinatorProvider.getCommitCoordinatorNameConfKey
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.ConcurrentWriteException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.util.Preconditions.checkArgument
import io.delta.kernel.internal.util.{FileNames, ManualClock}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.storage.LogStore
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.File
import java.util.{Collections, Optional}
import java.{lang, util}
import scala.collection.immutable.Seq
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._

class CoordinatedCommitsSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {

  def setupCoordinatedCommitsForTest(
      engine: Engine,
      tablePath: String,
      commitDatas: Seq[Seq[FilteredColumnarBatch]],
      coordinatorName: String = "tracking-in-memory",
      tableConfToOverwrite: String = "{}",
      versionConvertToCC: Long = 0L,
      coordinatedCommitNum: Long = 3L,
      checkpointVersion: Long = -1L,
      checkpointInterval: Long = -1L,
      backfillVersion: Long = -1L): Unit = {
    val table = Table.forPath(engine, tablePath)
    val totalCommitNum = coordinatedCommitNum + versionConvertToCC
    val handler = engine.getCommitCoordinatorClientHandler(
      coordinatorName,
      OBJ_MAPPER.readValue(tableConfToOverwrite, classOf[util.Map[String, String]]))
    val logPath = new Path(table.getPath(engine), "_delta_log")

    /** Rewrite the FS to CC conversion commit and move coordinated commits to _commits folder */
    (0L until totalCommitNum).foreach{ version =>

      if (version == versionConvertToCC) {
        appendData(
          engine,
          tablePath,
          isNewTable = version == 0L,
          testSchema,
          partCols = Seq.empty[String],
          data = Seq(Map.empty[String, Literal] -> commitDatas(version.toInt)),
          tableProperties = Map(
            COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> coordinatorName,
            COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> tableConfToOverwrite)
        )
      } else {
        appendData(
          engine,
          tablePath,
          isNewTable = version == 0L,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] ->  commitDatas(version.toInt))
        )
      }
      if (version == checkpointVersion) {
        table.checkpoint(engine, version)
      }
      if (checkpointInterval != -1 && version % checkpointInterval == 0) {
        table.checkpoint(engine, version)
      }
      if (version == backfillVersion) {
        handler.backfillToVersion(logPath.toString, Collections.emptyMap(), version, null)
      }
    }
  }

  def testWithCoordinatorCommits(
      testName: String,
      hadoopConf: Map[String, String] = Map.empty)(f: (String, Engine) => Unit): Unit = {
    test(testName) {
      InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
      withTempDirAndEngine(f, hadoopConf)
    }
  }

  testWithCoordinatorCommits(
      "0th commit happens via filesystem",
      trackingInMemoryBatchSizeConfig(batchSize = 5, name = "nobackfilling-commit-coordinator")) {
    (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val logPath = new KernelPath(table.getPath(engine), "_delta_log")
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty[String],
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "nobackfilling-commit-coordinator",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )

      assert(
        engine.getFileSystemClient.listFrom(FileNames.listingPrefix(logPath, 0L)).exists { f =>
          new Path(f.getPath).getName === "00000000000000000000.json"
        })
  }

  testWithCoordinatorCommits("basic write", trackingInMemoryBatchSizeConfig(2)) {
    (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val logPath = new KernelPath(table.getPath(engine), "_delta_log")
      val commitsDir = new File(FileNames.commitDirPath(logPath).toUri)
      val deltaDir = new File(logPath.toUri)

      val commitDatas = Seq.fill(2)(dataBatches1)
      setupCoordinatedCommitsForTest(
        engine,
        tablePath, commitDatas, coordinatedCommitNum = 2L)

      assert(getCommitVersions(commitsDir) === Array(1))
      assert(getCommitVersions(deltaDir) === Array(0))

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      assert(getCommitVersions(commitsDir) === Array(1, 2))
      assert(getCommitVersions(deltaDir) === Array(0, 1, 2))

      val snapshot = table.getLatestSnapshot(engine)
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows)

      checkAnswer(result, expectedAnswer)
  }

  testWithCoordinatorCommits("cold snapshot initialization", trackingInMemoryBatchSizeConfig(10)) {
    (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val commitDatas = Seq.fill(3)(dataBatches1)
      setupCoordinatedCommitsForTest(engine, tablePath, commitDatas)

      var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
      for (version <- 0L to 1L) {
        val snapshot = table.getSnapshotAsOfVersion(engine, version)
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, expectedAnswer)
        expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
      }

      TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
      val snapshot2 = table.getLatestSnapshot(engine)
      val result2 = readSnapshot(snapshot2, snapshot2.getSchema(engine), null, null, engine)
      checkAnswer(result2, expectedAnswer)
      assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "snapshot read should use coordinated commit related properties properly",
    Map(getCommitCoordinatorNameConfKey("test-coordinator") ->
      classOf[TestCommitCoordinatorBuilder].getName)) { (tablePath, engine) =>
    val commitDatas = Seq.fill(3)(dataBatches1)
    setupCoordinatedCommitsForTest(
      engine,
      tablePath,
      commitDatas,
      coordinatorName = "test-coordinator",
      tableConfToOverwrite =
        OBJ_MAPPER.writeValueAsString(TestCommitCoordinator.EXP_COORDINATOR_CONF))
    val table = Table.forPath(engine, tablePath)

    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
    val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
    val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
      dataBatches1.flatMap(_.toTestRows) ++
      dataBatches1.flatMap(_.toTestRows)
    checkAnswer(result, expectedAnswer)

    assert(snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
    assert(
      snapshot
        .getTableCommitCoordinatorClientHandlerOpt(engine)
        .get()
        .semanticEquals(
          engine.getCommitCoordinatorClientHandler(
            "test-coordinator", TestCommitCoordinator.EXP_COORDINATOR_CONF)))
    assert(COORDINATED_COMMITS_TABLE_CONF.fromMetadata(snapshot.getMetadata) ===
      TestCommitCoordinator.EXP_TABLE_CONF)

    assert(TrackingCommitCoordinatorClient.numCommitsCalled.get > 0)
    assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get > 0)
    assert(TestCommitCoordinator.numBackfillToVersionCalled > 0)
  }

  testWithCoordinatorCommits(
    "commit fails if we try to put bad value for COORDINATED_COMMITS_TABLE_CONF",
    trackingInMemoryBatchSizeConfig(10)) { (tablePath, engine) =>
    intercept[RuntimeException] {
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}",
          TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
            """{"key1": "string_value", "key2Int": "2""")
      )
    }
  }

  testWithCoordinatorCommits(
    "snapshot read with checkpoint before table converted to coordinated commit table",
    trackingInMemoryBatchSizeConfig(10)) { (tablePath, engine) =>
    val commitDatas = Seq.fill(4)(dataBatches1)
    setupCoordinatedCommitsForTest(
      engine,
      tablePath,
      commitDatas,
      versionConvertToCC = 2L,
      coordinatedCommitNum = 2L,
      checkpointVersion = 1L)

    val table = Table.forPath(engine, tablePath)
    var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
    for (version <- 0L to 2L) {
      val snapshot = table.getSnapshotAsOfVersion(engine, version)
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      checkAnswer(result, expectedAnswer)
      expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
    }

    TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
    val snapshot3 = table.getLatestSnapshot(engine)
    val result3 = readSnapshot(snapshot3, snapshot3.getSchema(engine), null, null, engine)
    checkAnswer(result3, expectedAnswer)
    assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "snapshot read with overlap between filesystem based commits and coordinated commits",
    trackingInMemoryBatchSizeConfig(10)) { (tablePath, engine) =>
    val commitDatas = Seq.fill(6)(dataBatches1)
    setupCoordinatedCommitsForTest(
      engine,
      tablePath,
      commitDatas,
      versionConvertToCC = 2L,
      coordinatedCommitNum = 4L,
      backfillVersion = 4L)

    val table = Table.forPath(engine, tablePath)
    var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
    for (version <- 0L to 4L) {
      val snapshot = table.getSnapshotAsOfVersion(engine, version)
      val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
      checkAnswer(result, expectedAnswer)
      expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
    }

    TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
    val snapshot5 = table.getLatestSnapshot(engine)
    val result5 = readSnapshot(snapshot5, snapshot5.getSchema(engine), null, null, engine)
    checkAnswer(result5, expectedAnswer)
    assert(TrackingCommitCoordinatorClient.numGetCommitsCalled.get === 1)
  }

  testWithCoordinatorCommits(
    "getSnapshotAt with coordinated commits enabled", trackingInMemoryBatchSizeConfig(10)) {
    (tablePath, engine) =>
      val commitDatas = Seq.fill(5)(dataBatches1)
      setupCoordinatedCommitsForTest(
        engine, tablePath, commitDatas, versionConvertToCC = 2L)

      val table = Table.forPath(engine, tablePath)
      var expectedAnswer: Seq[TestRow] = commitDatas.head.flatMap(_.toTestRows)
      for (version <- 0L to 4L) {
        val snapshot = table.getSnapshotAsOfVersion(engine, version)
        val result = readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine)
        checkAnswer(result, expectedAnswer)
        if (version != 4L) {
          expectedAnswer = expectedAnswer ++ commitDatas(version.toInt + 1).flatMap(_.toTestRows)
        }
      }
  }

  testWithCoordinatorCommits(
    "versionToLoad higher than possible", trackingInMemoryBatchSizeConfig(10)) {
    (tablePath, engine) =>
      val commitDatas = Seq.fill(5)(dataBatches1)
      setupCoordinatedCommitsForTest(
        engine, tablePath, commitDatas, versionConvertToCC = 2L)
      val table = Table.forPath(engine, tablePath)
      val e = intercept[RuntimeException] {
        table.getSnapshotAsOfVersion(engine, 5L)
      }
      assert(e.getMessage.contains(
        "Cannot load table version 5 as it does not exist. The latest available version is 4"))
  }

  testWithCoordinatorCommits(
      "snapshot is updated recursively when FS table is converted to commit-coordinator table",
      trackingInMemoryBatchSizeConfig(10)) { (tablePath, engine) =>
        val commitDatas = Seq.fill(5)(dataBatches1)
        setupCoordinatedCommitsForTest(
          engine,
          tablePath,
          commitDatas,
          versionConvertToCC = 2L)

        val table = Table.forPath(engine, tablePath)

        val snapshotV1 = table.getSnapshotAsOfVersion(engine, 1L).asInstanceOf[SnapshotImpl]
        assert(snapshotV1.getVersion(engine) === 1L)
        assert(!snapshotV1.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)

        val snapshotV4 = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
        assert(snapshotV4.getVersion(engine) === 4)
        assert(snapshotV4.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)
        // only delta 3/4 will be un-backfilled and should have two dots in filename (x.uuid.json)
        assert(
          snapshotV4
            .getLogSegment
            .deltas.count(f => new Path(f.getPath).getName.count(_ == '.') == 2) === 2)
    }

  testWithDifferentBackfillInterval(
      "post commit snapshot creation",
      "tracking-in-memory") {
    (tablePath, engine, backfillInterval) =>
      def getDeltasInPostCommitSnapshot(table: Table): Seq[String] = {
        table
          .getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
          .getLogSegment.deltas
          .map(f => new Path(f.getPath).getName.replace("0000000000000000000", "")).toList
      }

      val table = Table.forPath(engine, tablePath)
      // Commit 0
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )
      assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json"))

      // Commit 1
      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      ) // version 1
      val commit1 = if (backfillInterval < 2) "1.json" else "1.uuid-1.json"
      assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json", commit1))

      // Commit 2
      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      ) // version 2
      if (backfillInterval <= 2) {
        // backfill would have happened at commit 2. Next deltaLog.update will pickup the
        // backfilled files.
        assert(getDeltasInPostCommitSnapshot(table) === Seq("0.json", "1.json", "2.json"))
      } else {
        assert(getDeltasInPostCommitSnapshot(table) ===
          Seq("0.json", "1.uuid-1.json", "2.uuid-2.json"))
      }

      // Commit 3
      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      ) // version 3
      val commit3 = if (backfillInterval < 2) "3.json" else "3.uuid-3.json"
      if (backfillInterval <= 2) {
        assert(
          getDeltasInPostCommitSnapshot(table) === Seq("0.json", "1.json", "2.json", commit3))
      } else {
        assert(getDeltasInPostCommitSnapshot(table) ===
          Seq("0.json", "1.uuid-1.json", "2.uuid-2.json", commit3))
      }

      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows) ++
        dataBatches1.flatMap(_.toTestRows) ++
        dataBatches2.flatMap(_.toTestRows)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      checkAnswer(
        readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
  }


  testWithDifferentBackfillInterval(
      "Snapshot.ensureCommitFilesBackfilled", "tracking-in-memory") {
    (tablePath, engine, _) =>
      val table = Table.forPath(engine, tablePath)
      val logPath = new KernelPath(table.getPath(engine), "_delta_log")
      val commitDatas = Seq.fill(10)(dataBatches1)
      // Add 10 commits to the table
      setupCoordinatedCommitsForTest(
        engine, tablePath, commitDatas, coordinatedCommitNum = 10L)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

      snapshot.ensureCommitFilesBackfilled(engine)

      val commitFiles =
        engine.getFileSystemClient.listFrom(FileNames.listingPrefix(logPath, 0L))
          .filterNot(f => f.getPath.endsWith("_commits"))
          .map(_.getPath)
      val backfilledCommitFiles = (0 to 9).map(
        version => FileNames.deltaFile(logPath, version))
      assert(commitFiles.toSeq == backfilledCommitFiles)
  }

  testWithDifferentCheckpointVersion("checkpoint with coordinated commit") {
    // TODO: this test doesn't seem to be complete in the original PR, fix it
    (tablePath, engine, checkpointInterval) =>
      val table = Table.forPath(engine, tablePath)
      val commitDatas = Seq.fill(20)(dataBatches1)
      // Add 20 commits to the table
      setupCoordinatedCommitsForTest(
        engine,
        tablePath,
        commitDatas, coordinatedCommitNum = 20L, checkpointInterval = checkpointInterval)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val expectedAnswer = commitDatas.flatMap(_.flatMap(_.toTestRows))
      checkAnswer(
        readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
  }

  testWithCoordinatorCommits("table.getSnapshotAsOfVersion", trackingInMemoryBatchSizeConfig(10)) {
    (tablePath, engine) =>
      def checkGetSnapshotAt(engine: Engine, table: Table, version: Long): Unit = {
        var snapshot: SnapshotImpl = null

        snapshot = table.getSnapshotAsOfVersion(engine, version).asInstanceOf[SnapshotImpl]
        assert(snapshot.getVersion(engine) === version)

        val versionsInLogSegment = snapshot.getLogSegment.deltas.map(
          f => FileNames.deltaVersion(f.getPath))
        assert(versionsInLogSegment === (0L to version))
        val expectedAnswer = (1L to version + 1L).flatMap(_ => dataBatches1.flatMap(_.toTestRows))
        checkAnswer(
          readSnapshot(snapshot, snapshot.getSchema(engine), null, null, engine), expectedAnswer)
      }

      val table = Table.forPath(engine, tablePath)
      val commitDatas = Seq.fill(5)(dataBatches1)
      setupCoordinatedCommitsForTest(
        engine,
        tablePath,
        commitDatas,
        versionConvertToCC = 3L,
        coordinatedCommitNum = 2L)
      for (version <- 0L to 4L) {
        checkGetSnapshotAt(engine, table, version)
      }
  }

  val transferConfig = Map(
    getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
    getCommitCoordinatorNameConfKey("nobackfilling-commit-coordinator") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
    InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "10")
  testWithCoordinatorCommits(
    "transfer from one commit-coordinator to another commit-coordinator fails [CC-1 -> CC-2 fails]",
    transferConfig) {
    (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      enableCoordinatedCommits(engine, tablePath, "tracking-in-memory", isNewTable = true)
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(snapshot.getVersion(engine) === 0L)
      assert(snapshot.getTableCommitCoordinatorClientHandlerOpt(engine).isPresent)

      // Change commit-coordinator
      val ex = intercept[IllegalStateException] {
        enableCoordinatedCommits(engine, tablePath, "nobackfilling-commit-coordinator")
      }
      assert(ex.getMessage.contains(
        "from one commit-coordinator to another commit-coordinator is not allowed"))
  }

  testWithCoordinatorCommits(
      "FS -> CC upgrade is not retried on a conflict",
      trackingInMemoryBatchSizeConfig(10)) {
    (tablePath, engine) =>
      val txn = createTxn(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        Seq.empty,
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}"))

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      intercept[ConcurrentWriteException] {
        txn.commit(engine, emptyIterable()) // upgrade txn committed
      }
  }

  testWithCoordinatorCommits(
      "Conflict resolution should work with coordinated commits",
      trackingInMemoryBatchSizeConfig(10)) { (tablePath, engine) =>
    val table = TableImpl.forPath(engine, tablePath, () => System.currentTimeMillis)

    val commitDatas = Seq.fill(4)(dataBatches1)
    setupCoordinatedCommitsForTest(
      engine,
      tablePath,
      commitDatas,
      versionConvertToCC = 2L,
      coordinatedCommitNum = 2L)

    val startTime = System.currentTimeMillis()
    val clock = new ManualClock(startTime)
    val txn1 = createTxn(
      engine,
      tablePath,
      schema = testSchema,
      partCols = Seq.empty,
      clock = clock
    )
    clock.setTime(startTime)
    appendData(
      engine,
      tablePath,
      data = Seq(Map.empty[String, Literal] -> dataBatches2),
      clock = clock
    )
    clock.setTime(startTime - 1000)
    commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
    val ver4Snapshot = table.getSnapshotAsOfVersion(engine, 4L).asInstanceOf[SnapshotImpl]
    val ver5Snapshot = table.getSnapshotAsOfVersion(engine, 5L).asInstanceOf[SnapshotImpl]
    assert(
      ver5Snapshot.getTimestamp(engine) === ver4Snapshot.getTimestamp(engine) + 1)
  }

  private def trackingInMemoryBatchSizeConfig(
      batchSize: Int,
      name: String = "tracking-in-memory"): Map[String, String] = {
    Map(
      getCommitCoordinatorNameConfKey(name) ->
        classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> batchSize.toString)
  }

  def getCommitVersions(dir: File): Array[Long] = {
    dir
      .listFiles()
      .filterNot(f => f.getName.startsWith(".") && f.getName.endsWith(".crc"))
      .filterNot(f => f.getName.equals("_commits"))
      .map(_.getAbsolutePath)
      .sortBy(path => path).map { commitPath =>
        assert(FileNames.isCommitFile(commitPath))
        FileNames.deltaVersion(new KernelPath(commitPath))
      }
  }
}

object TestCommitCoordinator {
  val EXP_TABLE_CONF: util.Map[String, String] = Map(
    "tableKey1" -> "string_value",
    "tableKey2Int" -> "2",
    "tableKey3ComplexStr" -> "\"hello\""
  ).asJava

  val EXP_COORDINATOR_CONF: util.Map[String, String] = Map(
    "coordinatorKey1" -> "string_value",
    "coordinatorKey2Int" -> "2",
    "coordinatorKey3ComplexStr" -> "\"hello\"").asJava

  val COORDINATOR = new TrackingCommitCoordinatorClient(new TestCommitCoordinatorClient())

  // TODO: why is this here instead of the tracking coordinator?
  var numBackfillToVersionCalled = 0
}

/**
 * A [[CommitCoordinatorClient]] that tests can use to check the coordinator configuration and
 * table configuration.
 */
class TestCommitCoordinatorClient extends InMemoryCommitCoordinator(2) {
  override def registerTable(
      logPath: Path,
      tableIdentifier: Optional[TableIdentifier],
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): util.Map[String, String] = {
    super.registerTable(logPath, tableIdentifier, currentVersion, currentMetadata, currentProtocol)
    TestCommitCoordinator.EXP_TABLE_CONF
  }
  override def getCommits(
                           tableDesc: TableDescriptor,
                           startVersion: lang.Long,
                           endVersion: lang.Long = null): GetCommitsResponse = {
    checkArgument(tableDesc.getTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.getCommits(tableDesc, startVersion, endVersion)
  }
  override def commit(
                       logStore: LogStore,
                       hadoopConf: Configuration,
                       tableDesc: TableDescriptor,
                       commitVersion: Long,
                       actions: util.Iterator[String],
                       updatedActions: UpdatedActions): CommitResponse = {
    checkArgument(tableDesc.getTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.commit(logStore, hadoopConf, tableDesc, commitVersion, actions, updatedActions)
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      tableDesc: TableDescriptor,
      version: Long,
      // fix this shit
      lastKnownBackfilledVersion: java.lang.Long): Unit = {
    TestCommitCoordinator.numBackfillToVersionCalled += 1
    checkArgument(tableDesc.getTableConf == TestCommitCoordinator.EXP_TABLE_CONF)
    super.backfillToVersion(
      logStore,
      hadoopConf,
      tableDesc,
      version,
      lastKnownBackfilledVersion)
  }
}

class TestCommitCoordinatorBuilder(hadoopConf: Configuration)
    extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    checkArgument(conf == TestCommitCoordinator.EXP_COORDINATOR_CONF)
    TestCommitCoordinator.COORDINATOR
  }
  override def getName: String = "test-coordinator"
}
