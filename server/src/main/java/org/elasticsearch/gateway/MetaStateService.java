/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Handles writing and loading {@link MetaState}, {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService extends AbstractComponent implements IndexMetaDataWriter {
    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;

    public MetaStateService(Settings settings, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Loads the full state, which includes both the global state and all the indices meta data. <br>
     * When loading, manifest file is consulted (represented by {@link MetaState} class), to load proper generations. <br>
     * If there is no manifest file on disk, this method fallbacks to BWC mode, where latest generation of global and indices
     * metadata is loaded. Please note that currently where is no way to distinguish between manifest file being removed and manifest
     * file was not yet created. It means that this method always fallbacks to BWC mode, if there is no manifest file.
     *
     * @return tuple of {@link MetaState} and {@link MetaData} with global metadata and indices metadata. If there is no state on disk,
     * meta state with globalGeneration -1 and empty meta data is returned.
     * @throws IOException if some IOException when loading files occurs or there is no metadata referenced by manifest file.
     */
    Tuple<MetaState, MetaData> loadFullState() throws IOException {
        final MetaState metaState = loadMetaState();
        if (metaState == null) {
            MetaData globalMetaData =
                    MetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths()).v1();
            boolean isFreshStartup = globalMetaData == null;
            if (isFreshStartup == false && Version.CURRENT.major >= 8) {
                throw new IOException("failed to find manifest file, which is mandatory staring with ElasticSearch version 8.0");
            }
            return loadFullStateBWC();
        }
        final MetaData.Builder metaDataBuilder;
        final MetaData globalMetaData = MetaData.FORMAT.loadGeneration(logger, namedXContentRegistry, metaState.getGlobalStateGeneration(),
                nodeEnv.nodeDataPaths());
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            throw new IOException("failed to find global metadata [generation: " + metaState.getGlobalStateGeneration() + "]");
        }
        for (Map.Entry<Index, Long> entry : metaState.getIndices().entrySet()) {
            Index index = entry.getKey();
            long generation = entry.getValue();
            final String indexFolderName = index.getUUID();
            final IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadGeneration(logger, namedXContentRegistry, generation,
                    nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                throw new IOException("failed to find metadata for existing index [location: " + indexFolderName +
                        ", generation: " + generation + "]");
            }
        }
        return new Tuple<>(metaState, metaDataBuilder.build());
    }

    /**
     * Zen 1 BWC version of loading metadata from disk. See also {@link #loadFullState()}
     */
    private Tuple<MetaState, MetaData> loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        Tuple<MetaData, Long> metaDataAndGeneration =
                MetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        MetaData globalMetaData = metaDataAndGeneration.v1();
        long globalStateGeneration = metaDataAndGeneration.v2();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            Tuple<IndexMetaData, Long> indexMetaDataAndGeneration = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                    nodeEnv.resolveIndexFolder(indexFolderName));
            IndexMetaData indexMetaData = indexMetaDataAndGeneration.v1();
            long generation = indexMetaDataAndGeneration.v2();
            if (indexMetaData != null) {
                indices.put(indexMetaData.getIndex(), generation);
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        MetaState metaState = new MetaState(globalStateGeneration, indices);
        return new Tuple<>(metaState, metaDataBuilder.build());
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    public IndexMetaData loadIndexState(Index index) throws IOException {
        return IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index)).v1();
    }

    private MetaState loadMetaState() throws IOException {
        return MetaState.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths()).v1();
    }

    /**
     * Loads all indices states available on disk
     */
    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders(excludeIndexPathIdsPredicate)) {
            assert excludeIndexPathIdsPredicate.test(indexFolderName) == false :
                    "unexpected folder " + indexFolderName + " which should have been excluded";
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry,
                    nodeEnv.resolveIndexFolder(indexFolderName)).v1();
            if (indexMetaData != null) {
                final String indexPathId = indexMetaData.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetaDataList.add(indexMetaData);
                } else {
                    throw new IllegalStateException("[" + indexFolderName + "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    MetaData loadGlobalState() throws IOException {
        return MetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths()).v1();
    }

    /**
     * Writes manifest file (represented by {@link MetaState}) to disk.
     *
     * @throws WriteStateException if exception when writing state occurs. See also {@link WriteStateException#isDirty()}
     */
    public long writeMetaState(String reason, MetaState metaState) throws WriteStateException {
        logger.trace("[_meta] writing state, reason [{}]", reason);
        try {
            long generation = MetaState.FORMAT.write(metaState, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
            return generation;
        } catch (WriteStateException ex) {
            logger.warn("[_meta]: failed to write meta state", ex);
            throw ex;
        }
    }

    /**
     * Writes the index state.
     * <p>
     * This method is public for testing purposes.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new index state file is not yet referenced by manifest file.
     */
    public long writeIndex(String reason, IndexMetaData indexMetaData) throws WriteStateException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            long generation = IndexMetaData.FORMAT.write(indexMetaData,
                    nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written", index);
            return generation;
        } catch (WriteStateException ex) {
            logger.warn(() -> new ParameterizedMessage("[{}]: failed to write index state", index), ex);
            ex.resetDirty();
            throw ex;
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new global state file is not yet referenced by manifest file.
     */
    long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            long generation = MetaData.FORMAT.write(metaData, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written");
            return generation;
        } catch (WriteStateException ex) {
            logger.warn("[_global]: failed to write global state", ex);
            ex.resetDirty();
            throw ex;
        }
    }

    /**
     * Removes old state files in global state directory.
     *
     * @param currentGeneration current state generation to keep in the directory.
     */
    void cleanupGlobalState(long currentGeneration) {
        MetaData.FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.nodeDataPaths());
    }

    /**
     * Removes old state files in index directory.
     *
     * @param index             index to perform clean up on.
     * @param currentGeneration current state generation to keep in the index directory.
     */
    public void cleanupIndex(Index index, long currentGeneration) {
        IndexMetaData.FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.indexPaths(index));
    }

    /**
     * Removes old state files in meta state directory.
     *
     * @param currentGeneration current state generation to keep in the directory.
     */
    public void cleanupMetaState(long currentGeneration) {
        MetaState.FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.nodeDataPaths());
    }

    /**
     * Writes index metadata and updates manifest file accordingly.
     */
    public void writeIndexAndUpdateMetaState(String reason, IndexMetaData metaData) throws IOException {
        long generation = writeIndex(reason, metaData);
        MetaState metaState = loadMetaState();
        Map<Index, Long> indices = new HashMap<>(metaState.getIndices());
        indices.put(metaData.getIndex(), generation);
        metaState = new MetaState(metaState.getGlobalStateGeneration(), indices);
        long metaStateGeneration = writeMetaState(reason, metaState);
        cleanupIndex(metaData.getIndex(), generation);
        cleanupMetaState(metaStateGeneration);
    }

    /**
     * Writes global metadata and updates manifest file accordingly.
     */
    public void writeGlobalStateAndUpdateMetaState(String reason, MetaData metaData) throws IOException {
        long generation = writeGlobalState(reason, metaData);
        MetaState metaState = loadMetaState();
        metaState = new MetaState(generation, metaState.getIndices());
        long metaStateGeneration = writeMetaState(reason, metaState);
        cleanupGlobalState(generation);
        cleanupMetaState(metaStateGeneration);
    }
}