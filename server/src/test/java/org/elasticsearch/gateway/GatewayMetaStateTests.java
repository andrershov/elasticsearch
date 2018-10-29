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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.test.TestCustomMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GatewayMetaStateTests extends ESAllocationTestCase {

    private ClusterState noIndexClusterState(boolean masterEligible) {
        MetaData metaData = MetaData.builder().build();
        RoutingTable routingTable = RoutingTable.builder().build();

        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
    }

    private ClusterState clusterStateWithUnassignedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        MetaData metaData = MetaData.builder()
                .put(indexMetaData, false)
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(routingTable)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
    }

    private ClusterState clusterStateWithAssignedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
                .build());

        ClusterState oldClusterState = clusterStateWithUnassignedIndex(indexMetaData, masterEligible);
        RoutingTable routingTable = strategy.reroute(oldClusterState, "reroute").routingTable();

        MetaData metaDataNewClusterState = MetaData.builder()
                .put(oldClusterState.metaData().index("test"), false)
                .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
                .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithClosedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithAssignedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.CLOSE)
                        .numberOfShards(5).numberOfReplicas(2))
                .version(oldClusterState.metaData().version() + 1)
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaDataNewClusterState.index("test"))
                .build();

        return ClusterState.builder(oldClusterState).routingTable(routingTable)
                .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private ClusterState clusterStateWithJustOpenedIndex(IndexMetaData indexMetaData, boolean masterEligible) {
        ClusterState oldClusterState = clusterStateWithClosedIndex(indexMetaData, masterEligible);

        MetaData metaDataNewClusterState = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.OPEN)
                        .numberOfShards(5).numberOfReplicas(2))
                .version(oldClusterState.metaData().version() + 1)
                .build();

        return ClusterState.builder(oldClusterState)
                .metaData(metaDataNewClusterState).version(oldClusterState.getVersion() + 1).build();
    }

    private DiscoveryNodes.Builder generateDiscoveryNodes(boolean masterEligible) {
        Set<DiscoveryNode.Role> dataOnlyRoles = Collections.singleton(DiscoveryNode.Role.DATA);
        return DiscoveryNodes.builder().add(newNode("node1", masterEligible ? MASTER_DATA_ROLES : dataOnlyRoles))
                .add(newNode("master_node", MASTER_DATA_ROLES)).localNodeId("node1").masterNodeId(masterEligible ? "node1" : "master_node");
    }

    private Set<Index> randomPrevWrittenIndices(IndexMetaData indexMetaData) {
        if (randomBoolean()) {
            return Collections.singleton(indexMetaData.getIndex());
        } else {
            return Collections.emptySet();
        }
    }

    private IndexMetaData createIndexMetaData(String name) {
        return IndexMetaData.builder(name).
                settings(settings(Version.CURRENT)).
                numberOfShards(5).
                numberOfReplicas(2).
                build();
    }

    public void testGetRelevantIndices_master_unassignedShards() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithUnassignedIndex(indexMetaData, true),
                noIndexClusterState(true),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndices_data_unassignedShards() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithUnassignedIndex(indexMetaData, false),
                noIndexClusterState(false),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndices_assignedShards() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        boolean masterEligible = randomBoolean();
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithAssignedIndex(indexMetaData, masterEligible),
                clusterStateWithUnassignedIndex(indexMetaData, masterEligible),
                randomPrevWrittenIndices(indexMetaData));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndices_data_isClosedPrevWritten() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithClosedIndex(indexMetaData, false),
                clusterStateWithAssignedIndex(indexMetaData, false),
                Collections.singleton(indexMetaData.getIndex()));
        assertThat(indices.size(), equalTo(1));
    }

    public void testGetRelevantIndices_data_isClosedPrevNotWritten() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithJustOpenedIndex(indexMetaData, false),
                clusterStateWithClosedIndex(indexMetaData, false),
                Collections.emptySet());
        assertThat(indices.size(), equalTo(0));
    }

    public void testGetRelevantIndices_data_wasClosedPrevWritten() {
        IndexMetaData indexMetaData = createIndexMetaData("test");
        Set<Index> indices = GatewayMetaState.getRelevantIndices(
                clusterStateWithJustOpenedIndex(indexMetaData, false),
                clusterStateWithClosedIndex(indexMetaData, false),
                Collections.singleton(indexMetaData.getIndex()));
        assertThat(indices.size(), equalTo(1));
    }

    public void testResolveStatesToBeWritten() throws WriteStateException {
        Map<Index, Long> indices = new HashMap<>();
        Set<Index> relevantIndices = new HashSet<>();

        IndexMetaData removedIndex = createIndexMetaData("removed_index");
        indices.put(removedIndex.getIndex(), 1L);

        IndexMetaData versionChangedIndex = createIndexMetaData("version_changed_index");
        indices.put(versionChangedIndex.getIndex(), 2L);
        relevantIndices.add(versionChangedIndex.getIndex());

        IndexMetaData notChangedIndex = createIndexMetaData("not_changed_index");
        indices.put(notChangedIndex.getIndex(), 3L);
        relevantIndices.add(notChangedIndex.getIndex());

        IndexMetaData newIndex = createIndexMetaData("new_index");
        relevantIndices.add(newIndex.getIndex());

        MetaData oldMetaData = MetaData.builder()
                .put(removedIndex, false)
                .put(versionChangedIndex, false)
                .put(notChangedIndex, false)
                .build();

        MetaData newMetaData = MetaData.builder()
                .put(versionChangedIndex, true)
                .put(notChangedIndex, false)
                .put(newIndex, false)
                .build();

        IndexMetaData newVersionChangedIndex = newMetaData.index(versionChangedIndex.getIndex());


        List<GatewayMetaState.IndexMetaDataAction> actions =
                GatewayMetaState.resolveIndexMetaDataActions(indices, relevantIndices, oldMetaData, newMetaData);

        List<Runnable> cleanupActions = new ArrayList<>();

        assertThat(actions, hasSize(3));

        for (GatewayMetaState.IndexMetaDataAction action : actions) {
            if (action instanceof GatewayMetaState.KeepPreviousGeneration) {
                assertThat(action.getIndex(), equalTo(notChangedIndex.getIndex()));
                assertThat(action.execute(null, cleanupActions), equalTo(3L));
            }
            if (action instanceof GatewayMetaState.WriteNewIndexMetaData) {
                assertThat(action.getIndex(), equalTo(newIndex.getIndex()));
                action.execute(new IndexMetaDataWriter() {
                    @Override
                    public long writeIndex(String reason, IndexMetaData indexMetaData) {
                        assertThat(reason, equalTo("freshly created"));
                        assertThat(indexMetaData, equalTo(newIndex));
                        return 0L;
                    }

                    @Override
                    public void cleanupIndex(Index index, long currentGeneration) {
                        assertThat(index, equalTo(newIndex.getIndex()));
                        assertThat(currentGeneration, equalTo(0L));
                    }
                }, cleanupActions);
            }
            if (action instanceof GatewayMetaState.WriteChangedIndexMetaData) {
                assertThat(action.getIndex(), equalTo(newVersionChangedIndex.getIndex()));
                action.execute(new IndexMetaDataWriter() {
                    @Override
                    public long writeIndex(String reason, IndexMetaData indexMetaData) {
                        assertThat(reason, containsString(Long.toString(versionChangedIndex.getVersion())));
                        assertThat(reason, containsString(Long.toString(newVersionChangedIndex.getVersion())));
                        assertThat(indexMetaData, equalTo(newVersionChangedIndex));
                        return 3L;
                    }

                    @Override
                    public void cleanupIndex(Index index, long currentGeneration) {
                        assertThat(index, equalTo(newVersionChangedIndex.getIndex()));
                        assertThat(currentGeneration, equalTo(3L));
                    }
                }, cleanupActions);
            }
        }

        assertThat(cleanupActions, hasSize(2));
        for (Runnable cleanupAction : cleanupActions) {
            cleanupAction.run();
        }
    }

    public void testAddCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
    }

    public void testRemoveCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.remove(CustomMetaData1.TYPE);
                return customs;
            }),
            Collections.emptyList()
        );
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNull(upgrade.custom(CustomMetaData1.TYPE));
    }

    public void testUpdateCustomMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.singletonList(customs -> {
                customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                return customs;
            }),
            Collections.emptyList()
        );

        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
    }


    public void testUpdateTemplateMetaDataOnUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                templates -> {
                    templates.put("added_test_template", IndexTemplateMetaData.builder("added_test_template")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false))).build());
                    return templates;
                }
            ));

        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetaDataUpgrade() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList(), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade == metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataValidation() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(
            customs -> {
                throw new IllegalStateException("custom meta data too old");
            }
        ), Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testMultipleCustomMetaDataUpgrade() throws Exception {
        final MetaData metaData;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metaData = randomMetaData(new CustomMetaData1("data1"), new CustomMetaData2("data2"));
                break;
            case 1:
                metaData = randomMetaData(randomBoolean() ? new CustomMetaData1("data1") : new CustomMetaData2("data2"));
                break;
            case 2:
                metaData = randomMetaData();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Arrays.asList(
                customs -> {
                    customs.put(CustomMetaData1.TYPE, new CustomMetaData1("modified_data1"));
                    return customs;
                },
                customs -> {
                    customs.put(CustomMetaData2.TYPE, new CustomMetaData1("modified_data2"));
                    return customs;
                }
            ), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.custom(CustomMetaData1.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData1.TYPE)).getData(), equalTo("modified_data1"));
        assertNotNull(upgrade.custom(CustomMetaData2.TYPE));
        assertThat(((TestCustomMetaData) upgrade.custom(CustomMetaData2.TYPE)).getData(), equalTo("modified_data2"));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testIndexMetaDataUpgrade() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList(), Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(true), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertFalse(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataNoChange() throws Exception {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(HashMap::new),
            Collections.singletonList(HashMap::new));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade == metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testIndexTemplateValidation() throws Exception {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Collections.singletonList(
                customs -> {
                    throw new IllegalStateException("template is incompatible");
                }));
        String message = expectThrows(IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader)).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }


    public void testMultipleIndexTemplateUpgrade() throws Exception {
        final MetaData metaData;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metaData = randomMetaDataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metaData = randomMetaDataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metaData = randomMetaData();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(
            Collections.emptyList(),
            Arrays.asList(
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template1", IndexTemplateMetaData.builder("template1")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                        .build());
                    return indexTemplateMetaDatas;

                },
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template2", IndexTemplateMetaData.builder("template2")
                        .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    return indexTemplateMetaDatas;

                }
            ));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertTrue(upgrade != metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    private static class MockMetaDataIndexUpgradeService extends MetaDataIndexUpgradeService {
        private final boolean upgrade;

        MockMetaDataIndexUpgradeService(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData, Version minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetaData.builder(indexMetaData).build() : indexMetaData;
        }
    }

    private static class CustomMetaData1 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_1";

        protected CustomMetaData1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetaData2 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_2";

        protected CustomMetaData2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static MetaData randomMetaData(TestCustomMetaData... customMetaDatas) {
        MetaData.Builder builder = MetaData.builder();
        for (TestCustomMetaData customMetaData : customMetaDatas) {
            builder.putCustom(customMetaData.getWriteableName(), customMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static MetaData randomMetaDataWithIndexTemplates(String... templates) {
        MetaData.Builder builder = MetaData.builder();
        for (String template : templates) {
            IndexTemplateMetaData templateMetaData = IndexTemplateMetaData.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                .build();
            builder.put(templateMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }
}
