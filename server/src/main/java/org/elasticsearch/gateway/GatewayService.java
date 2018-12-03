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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> EXPECTED_NODES_SETTING =
        Setting.intSetting("gateway.expected_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING =
        Setting.intSetting("gateway.expected_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.expected_master_nodes", -1, -1, Property.NodeScope);
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING =
        Setting.positiveTimeSetting("gateway.recover_after_time", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_master_nodes", 0, 0, Property.NodeScope);

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true,
        false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final ThreadPool threadPool;

    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterMasterNodes;
    private final int expectedMasterNodes;

    private final Runnable recoveryRunnable;

    private final AtomicBoolean recovered = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(final Settings settings, final AllocationService allocationService, final ClusterService clusterService,
                          final ThreadPool threadPool,
                          final TransportNodesListGatewayMetaState listGatewayMetaState,
                          final IndicesService indicesService, final Discovery discovery) {
        super(settings);
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = EXPECTED_NODES_SETTING.get(settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);
        this.expectedMasterNodes = EXPECTED_MASTER_NODES_SETTING.get(settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        // default the recover after master nodes to the minimum master nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(settings)) {
            recoverAfterMasterNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(settings);
        } else {
            // TODO: change me once the minimum_master_nodes is changed too
            recoverAfterMasterNodes = settings.getAsInt("discovery.zen.minimum_master_nodes", -1);
        }

        if (discovery instanceof Coordinator) {
            recoveryRunnable = () ->
                    clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask());
        } else {
            final Gateway gateway = new Gateway(settings, clusterService, listGatewayMetaState);
            recoveryRunnable = () ->
                    gateway.performStateRecovery(new GatewayRecoveryListener());
        }
        this.indicesService = indicesService;
    }

    @Override
    protected void doStart() {
        // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final ClusterState state = event.state();

        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // not our job to recover
            return;
        }
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }

        final DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getMasterNodeId() == null) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getMasterAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug("not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getMasterAndDataNodes().size(), recoverAfterNodes);
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug("not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(), recoverAfterDataNodes);
        } else if (recoverAfterMasterNodes != -1 && nodes.getMasterNodes().size() < recoverAfterMasterNodes) {
            logger.debug("not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
                nodes.getMasterNodes().size(), recoverAfterMasterNodes);
        } else {
            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
                // no expected is set, honor the setting if they are there
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else {
                // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                enforceRecoverAfterTime = false;
                reason = "";
                if (expectedNodes != -1 && (nodes.getMasterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedNodes + "] nodes, but only have [" + nodes.getMasterAndDataNodes().size() + "]";
                } else if (expectedDataNodes != -1 && (nodes.getDataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
                } else if (expectedMasterNodes != -1 && (nodes.getMasterNodes().size() < expectedMasterNodes)) {
                    // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.getMasterNodes().size() + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(final boolean enforceRecoverAfterTime, final String reason) {
        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(recoverAfterTime, ThreadPool.Names.GENERIC, () -> {
                    if (recovered.compareAndSet(false, true)) {
                        logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                        recoveryRunnable.run();
                    }
                });
            }
        } else {
            if (recovered.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(final Exception e) {
                        logger.warn("Recovery failed", e);
                        // we reset `recovered` in the listener don't reset it here otherwise there might be a race
                        // that resets it to false while a new recover is already running?
                        GatewayService.this.onFailure("state recovery failed: " + e.getMessage());
                    }

                    @Override
                    protected void doRun() {
                        recoveryRunnable.run();
                    }
                });
            }
        }
    }

    private void onFailure(final String message) {
        recovered.set(false);
        scheduledRecovery.set(false);
        // don't remove the block here, we don't want to allow anything in such a case
        logger.info("metadata state not restored, reason: {}", message);
    }

    static class Updaters {
        static ClusterState upgradeAndArchiveUnknownOrInvalidSettings(final ClusterState clusterState,
                                                                      final ClusterSettings clusterSettings) {
            final MetaData.Builder metaDataBuilder = MetaData.builder(clusterState.metaData());

            metaDataBuilder.persistentSettings(
                    clusterSettings.archiveUnknownOrInvalidSettings(
                            clusterSettings.upgradeSettings(metaDataBuilder.persistentSettings()),
                            e -> logUnknownSetting("persistent", e),
                            (e, ex) -> logInvalidSetting("persistent", e, ex)));
            metaDataBuilder.transientSettings(
                    clusterSettings.archiveUnknownOrInvalidSettings(
                            clusterSettings.upgradeSettings(metaDataBuilder.transientSettings()),
                            e -> logUnknownSetting("transient", e),
                            (e, ex) -> logInvalidSetting("transient", e, ex)));
            return ClusterState.builder(clusterState).metaData(metaDataBuilder).build();
        }

        private static void logUnknownSetting(final String settingType, final Map.Entry<String, String> e) {
            logger.warn("ignoring unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
        }

        private static void logInvalidSetting(final String settingType, final Map.Entry<String, String> e,
                                              final IllegalArgumentException ex) {
            logger.warn(() -> new ParameterizedMessage("ignoring invalid {} setting: [{}] with value [{}]; archiving",
                    settingType, e.getKey(), e.getValue()), ex);
        }

        static ClusterState recoverClusterBlocks(final ClusterState state) {
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(state.blocks());

            if (MetaData.SETTING_READ_ONLY_SETTING.get(state.metaData().settings())) {
                blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
            }

            if (MetaData.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(state.metaData().settings())) {
                blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            }

            for (final IndexMetaData indexMetaData : state.metaData()) {
                blocks.addBlocks(indexMetaData);
            }

            return ClusterState.builder(state).blocks(blocks).build();
        }

        static ClusterState closeBadIndices(final ClusterState clusterState, final IndicesService indicesService) {
            final MetaData.Builder builder = MetaData.builder(clusterState.metaData()).removeAllIndices();

            for (IndexMetaData metaData : clusterState.metaData()) {
                try {
                    if (metaData.getState() == IndexMetaData.State.OPEN) {
                        // verify that we can actually create this index - if not we recover it as closed with lots of warn logs
                        indicesService.verifyIndexMetadata(metaData, metaData);
                    }
                } catch (final Exception e) {
                    final Index electedIndex = metaData.getIndex();
                    logger.warn(() -> new ParameterizedMessage("recovering index {} failed - recovering as closed", electedIndex), e);
                    metaData = IndexMetaData.builder(metaData).state(IndexMetaData.State.CLOSE).build();
                }
                builder.put(metaData, false);
            }

            return ClusterState.builder(clusterState).metaData(builder).build();
        }

        static ClusterState updateRoutingTable(final ClusterState state) {
            // initialize all index routing tables as empty
            final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(state.routingTable());
            for (final ObjectCursor<IndexMetaData> cursor : state.metaData().indices().values()) {
                routingTableBuilder.addAsRecovery(cursor.value);
            }
            // start with 0 based versions for routing table
            routingTableBuilder.version(0);
            return ClusterState.builder(state).routingTable(routingTableBuilder.build()).build();
        }

        static ClusterState removeStateNotRecoveredBlock(final ClusterState state) {
            return ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder()
                            .blocks(state.blocks()).removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
                    .build();
        }

        static ClusterState mixCurrentStateAndRecoveredState(final ClusterState currentState, final ClusterState recoveredState) {
            assert currentState.metaData().indices().isEmpty();

            final ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                    .blocks(currentState.blocks())
                    .blocks(recoveredState.blocks());

            final MetaData.Builder metaDataBuilder = MetaData.builder(recoveredState.metaData());
            // automatically generate a UID for the metadata if we need to
            metaDataBuilder.generateClusterUuidIfNeeded();

            for (final IndexMetaData indexMetaData : recoveredState.metaData()) {
                metaDataBuilder.put(indexMetaData, false);
            }

            return ClusterState.builder(currentState)
                    .blocks(blocks)
                    .metaData(metaDataBuilder)
                    .build();
        }

    }

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(final ClusterState currentState) {
            final ClusterState newState = Function.<ClusterState>identity()
                    .andThen(state -> Updaters.closeBadIndices(state, indicesService))
                    .andThen(state -> Updaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
                    .andThen(Updaters::recoverClusterBlocks)
                    .andThen(Updaters::updateRoutingTable)
                    .andThen(Updaters::removeStateNotRecoveredBlock)
                    .apply(currentState);

            return allocationService.reroute(newState, "state recovered");
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metaData().indices().size());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            GatewayService.this.onFailure("failed to update cluster state");
        }
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask() {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    final ClusterState updatedState = Updaters.mixCurrentStateAndRecoveredState(currentState, recoveredState);
                    return super.execute(updatedState);
                }
            });
        }

        @Override
        public void onFailure(final String msg) {
            GatewayService.this.onFailure(msg);
        }

    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

}
