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
package io.delta.kernel.defaults

import io.delta.kernel.Table
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.replay.ActionWrapper
import io.delta.kernel.utils.CloseableIterator
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite

class TableSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils with SQLHelper {
  // scalastyle:off sparkimplicits
  // scalastyle:on sparkimplicits

  test("getChanges(startVersion) - simple ") {
    withTempDir { tblDir =>
      val tablePath = tblDir.getCanonicalPath
      spark.sql(s"CREATE TABLE delta.`$tablePath`(c1 long, c2 STRING) USING delta")

      def insertData(data1: Int, data2: Int): Unit = {
        spark.sql(
          s"""insert into delta.`$tablePath` values
             |(${data1}, '${data1}'),
             |(${data2}, '${data2}')
             |""".stripMargin)
      }

      insertData(data1 = 1, data2 = 2) // version 1
      insertData(data1 = 3, data2 = 4) // version 2
      insertData(data1 = 5, data2 = 6) // version 3
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE c1 = 3") // version 4
      insertData(data1 = 7, data2 = 8) // version 5
      spark.sql(s"OPTIMIZE delta.`$tablePath`") // version 6

      val table = Table.forPath(defaultEngine, tablePath)
      val snapshot = table.getLatestSnapshot(defaultEngine)
      val streamingState = snapshot.getStreamingState(defaultEngine)

      def verifyAddCount(
          expectedAddCount: Int,
          expectedRemoveCount: Int,
          changes: CloseableIterator[ActionWrapper]): Unit = {
        var numberOfAdds = 0
        var numberOfRemoves = 0
        while (changes.hasNext) {
          val rows = changes.next().getColumnarBatch.getRows
          while (rows.hasNext) {
            val row = rows.next()
            if (!row.isNullAt(0)) {
              numberOfAdds += 1
            }
            if (!row.isNullAt(1)) {
              numberOfRemoves += 1
            }
          }
        }

        assert(numberOfAdds === expectedAddCount)
        assert(numberOfRemoves === expectedRemoveCount)
      }


      val changesSinceV1 = table.getChanges(
        defaultEngine,
        streamingState,
        Table.StreamingRemoveHandlingPolicy.IGNORE_REMOVED_FILES,
        3, /* startVersion */
        6 /* endVersion */)
      verifyAddCount(
        expectedAddCount = 4,
        expectedRemoveCount = 5,
        changesSinceV1)


      // throw error when remove file is encountered
      val ex = intercept[KernelException] {
        val changesSinceV1 = table.getChanges(
          defaultEngine,
          streamingState,
          Table.StreamingRemoveHandlingPolicy.ERROR_ON_REMOVED_FILES,
          2, /* startVersion */
          6 /* endVersion */)
        while (changesSinceV1.hasNext) {
          changesSinceV1.next()
        }
      }
      assert(ex.getMessage.contains("Encountered a remove file"))
    }
  }

  test("getChanges(startVersion, endVersion) - schema change") {
    withTempDir { tblDir =>
      val tablePath = tblDir.getCanonicalPath
      spark.sql(s"CREATE TABLE delta.`$tablePath`(c1 long, c2 STRING) USING delta")

      def insertData(data1: Int, data2: Int): Unit = {
        spark.sql(
          s"""insert into delta.`$tablePath` values
             |(${data1}, '${data1}'),
             |(${data2}, '${data2}')
             |""".stripMargin)
      }

      def insertNewData(data1: Int, data2: Int): Unit = {
        spark.sql(
          s"""insert into delta.`$tablePath` values
             |(${data1}, '${data1}', ${data1}),
             |(${data2}, '${data2}', ${data2})
             |""".stripMargin)
      }

      insertData(data1 = 1, data2 = 2) // version 1
      insertData(data1 = 3, data2 = 4) // version 2
      insertData(data1 = 5, data2 = 6) // version 3
      spark.sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMNS (newCol LONG)") // version 4
      insertNewData(data1 = 7, data2 = 8) // version 5
      spark.sql(s"OPTIMIZE delta.`$tablePath`") // version 6

      val table = Table.forPath(defaultEngine, tablePath)
      val snapshot = table.getSnapshotAsOfVersion(defaultEngine, 2)
      val streamingState = snapshot.getStreamingState(defaultEngine)


      val changesSinceV1 = table.getChanges(
        defaultEngine,
        streamingState,
        Table.StreamingRemoveHandlingPolicy.IGNORE_REMOVED_FILES,
        2, /* startVersion */
        6 /* endVersion */
      )

      val ex = intercept[KernelException] {
        while (changesSinceV1.hasNext) {
          changesSinceV1.next()
        }
      }
      assert(ex.getMessage.contains("Metadata has changed between versions."))
    }
  }


}
