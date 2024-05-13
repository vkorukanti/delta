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
package io.delta.kernel.defaults.engine;

import static java.util.Objects.requireNonNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.*;

/**
 * Default implementation of {@link Engine} based on Hadoop APIs.
 */
public class DefaultEngine implements Engine {
    private final Configuration hadoopConf;
    private final FileSystemProvider fsProvider;


    /**
     * Create an instance of {@link DefaultEngine}.
     *
     * @param hadoopConf Hadoop configuration to use.
     * @param fsProvider {@link FileSystem} provider.
     */
    protected DefaultEngine(Configuration hadoopConf, FileSystemProvider fsProvider) {
        this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
        this.fsProvider = requireNonNull(fsProvider, "fileSystemProvider is null");
    }

    protected DefaultEngine(Configuration hadoopConf) {
        this(hadoopConf, new FileSystemProvider() {});
    }

    @Override
    public ExpressionHandler getExpressionHandler() {
        return new DefaultExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler() {
        return new DefaultJsonHandler(hadoopConf, fsProvider);
    }

    @Override
    public FileSystemClient getFileSystemClient() {
        return new DefaultFileSystemClient(hadoopConf, fsProvider);
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return new DefaultParquetHandler(hadoopConf, fsProvider);
    }

    /**
     * Create an instance of {@link DefaultEngine}.
     *
     * @param hadoopConf Hadoop configuration to use.
     * @return an instance of {@link DefaultEngine}.
     */
    @Evolving
    public static DefaultEngine create(Configuration hadoopConf) {
        return new DefaultEngine(hadoopConf, new FileSystemProvider() {});
    }

    /**
     * Create an instance of {@link DefaultEngine} with a custom file system provider.
     *
     * @param hadoopConf Hadoop configuration to use.
     * @param fsProvider {@link FileSystem} provider. Whenever a {@link FileSystem} is required by
     *                   the engine, the provider will be used to create it.
     * @return an instance of {@link DefaultEngine}.
     */
    @Evolving
    public static DefaultEngine create(Configuration hadoopConf, FileSystemProvider fsProvider) {
        return new DefaultEngine(hadoopConf, fsProvider);
    }
}
