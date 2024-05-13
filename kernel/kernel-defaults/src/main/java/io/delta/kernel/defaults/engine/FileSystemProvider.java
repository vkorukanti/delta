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
package io.delta.kernel.defaults.engine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.annotation.Evolving;

/**
 * Interface for providing a Hadoop {@link FileSystem} instance for a given {@link Path} and
 * {@link Configuration}. Connectors can override this method to provide a custom implementation
 * that takes into specific configurations or authentication mechanisms.
 * <p>
 * E.g. The connector may want to create once instance per each request and not share it across
 * requests. In this case, the connector can override this method to create a new instance for each
 * request that fits within the scope of the request.
 */
@Evolving
public interface FileSystemProvider {
    /**
     * Get a {@link FileSystem} instance for the given {@link Path} and {@link Configuration}.
     *
     * @param hadoopConf Hadoop configuration
     * @param path       Path to get the {@link FileSystem} for
     * @return {@link FileSystem} instance
     * @throws IOException For any I/O error while getting the {@link FileSystem}
     */
    default FileSystem getFileSystem(Configuration hadoopConf, Path path) throws IOException {
        return FileSystem.get(path.toUri(), hadoopConf);
    }
}
