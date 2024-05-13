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
package io.delta.kernel.defaults.engine

import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.types.{StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional

class DefaultEngineSuite extends AnyFunSuite with TestUtils {
  test("custom fs provider - json handler") {
    val customFsProvider = new CustomFsProvider
    val engine = DefaultEngine.create(new Configuration, customFsProvider)

    val testFiles = engine.getFileSystemClient
      .listFrom(getTestResourceFilePath("json-files/1.json"))

    engine.getJsonHandler
      .readJsonFiles(
        testFiles,
        new StructType().add("path", StringType.STRING),
        Optional.empty()
      ).toSeq.map(batch => TestRow(batch.getRows.next))

    assert(customFsProvider.callCount == 3) // 1 for each file under the `json-files` directory
  }
}

/**
 * Custom FileSystemProvider to test the number of times the getFileSystem method is called.
 */
class CustomFsProvider extends FileSystemProvider {
  var callCount: Int = 0;

  override def getFileSystem(hadoopConf: Configuration, path: Path): FileSystem = {
    this.callCount += 1
    super.getFileSystem(hadoopConf, path)
  }
}
