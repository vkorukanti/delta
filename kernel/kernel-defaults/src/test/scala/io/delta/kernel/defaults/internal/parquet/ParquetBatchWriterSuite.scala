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

package io.delta.kernel.defaults.internal.parquet;


import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.util.{InternalUtils, Utils}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.{IntegerType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional
import scala.collection.JavaConverters._

class ParquetBatchWriterSuite extends AnyFunSuite with TestUtils {
  val hadoopConf = new Configuration()

  test("writing - basic") {
    withTempDir { tempDir =>
      val dir = tempDir.getAbsolutePath
      val parquetWriter = new ParquetBatchWriter(
        hadoopConf,
        new Path(dir),
        200, /* maxFileSize */
        new StructType()
      )

      val schema = new StructType().add("intCol", INTEGER)

      val intVector1 = testColumnVector(200, INTEGER)
      val batch1: ColumnarBatch =
        new DefaultColumnarBatch(intVector1.getSize, schema, Seq(intVector1).toArray)
      val filtredBatch1 = new FilteredColumnarBatch(batch1, Optional.empty())

      val intVector2 = testColumnVector(400, INTEGER)
      val batch2: ColumnarBatch =
        new DefaultColumnarBatch(intVector2.getSize, schema, Seq(intVector2).toArray)
      val filtredBatch2 = new FilteredColumnarBatch(batch2, Optional.empty())

      val closebleIterBatches =
        Utils.toCloseableIterator(Seq(filtredBatch1, filtredBatch2).toIterator.asJava)

      val resultIter = parquetWriter.write(closebleIterBatches)

      while (resultIter.hasNext) {
        resultIter.next()
      }
    }
  }
}
