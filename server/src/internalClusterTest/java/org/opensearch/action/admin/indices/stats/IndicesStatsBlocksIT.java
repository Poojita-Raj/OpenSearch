/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

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

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.stats;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.English;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.common.io.PathUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.CorruptionUtils;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.FS;
import static org.opensearch.cluster.metadata.IndexMetadata.*;
import static org.opensearch.common.util.CollectionUtils.iterableAsArrayList;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class IndicesStatsBlocksIT extends OpenSearchIntegTestCase {

    private Path getPathToShardData(String indexName, String dirSuffix) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[] { indexName }, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        ShardId shardId = shardRouting.shardId();
        return getPathToShardData(nodeId, shardId, dirSuffix);
    }

    public static Path getPathToShardData(String nodeId, ShardId shardId, String shardPathSubdirectory) {
        final NodesStatsResponse nodeStatsResponse = client().admin().cluster().prepareNodesStats(nodeId).addMetric(FS.metricName()).get();
        final Set<Path> paths = StreamSupport.stream(nodeStatsResponse.getNodes().get(0).getFs().spliterator(), false)
            .map(
                nodePath -> PathUtils.get(nodePath.getPath())
                    .resolve(NodeEnvironment.INDICES_FOLDER)
                    .resolve(shardId.getIndex().getUUID())
                    .resolve(Integer.toString(shardId.getId()))
                    .resolve(shardPathSubdirectory)
            )
            .filter(Files::isDirectory)
            .collect(Collectors.toSet());
        assertThat(paths, hasSize(1));
        return paths.iterator().next();
    }
    public void testIndicesStatsWithBlocks() throws Exception {
        createIndex("ro");
        ensureGreen("ro");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("ro", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("ro").execute().actionGet();
        assertEquals(0, indicesStatsResponse.getPrimaries().getCorruptionStats().currentCorruptions());
        final Path indexDir = getPathToShardData("ro", ShardPath.INDEX_FOLDER_NAME);
        CorruptionUtils.corruptIndex(random(),indexDir, false);
        try {
            assertBusy(() -> {
                final ClusterAllocationExplanation explanation = client().admin()
                    .cluster()
                    .prepareAllocationExplain()
                    .setIndex("ro")
                    .setShard(0)
                    .setPrimary(true)
                    .get()
                    .getExplanation();

                final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
                assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
                assertThat(
                    shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                    equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
                );
            });
        } catch (Exception e) {
        }
        IndicesStatsResponse indicesStatsResponse3 = client().admin().indices().prepareStats("ro").execute().actionGet()assertEquals(1, indicesStatsResponse3.getPrimaries().getCorruptionStats().currentCorruptions());
        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("ro", blockSetting);
                IndicesStatsResponse indicesStatsResponse2 = client().admin().indices().prepareStats("ro").execute().actionGet();
                assertEquals(0, indicesStatsResponse2.getPrimaries().getCorruptionStats().currentCorruptions());
                assertNotNull(indicesStatsResponse2.getIndex("ro"));
            } finally {
                disableIndexBlock("ro", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
            client().admin().indices().prepareStats("ro").execute().actionGet();
            fail("Exists should fail when " + IndexMetadata.SETTING_BLOCKS_METADATA + " is true");
        } catch (ClusterBlockException e) {
            // Ok, a ClusterBlockException is expected
        } finally {
            disableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
        }
    }
}
