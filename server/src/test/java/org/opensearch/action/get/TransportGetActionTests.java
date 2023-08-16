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

package org.opensearch.action.get;

import org.opensearch.Version;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.tasks.Task;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.common.UUIDs.randomBase64UUID;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static TransportService transportService;
    private static ClusterService clusterService;

    private static ClusterService clusterService2;
    private static TransportGetAction transportAction;

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(TransportGetActionTests.class.getSimpleName());

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(
                Settings.builder().put("node.name", "node1").build(),
                boundAddress.publishAddress(),
                randomBase64UUID()
            ),
            null,
            emptySet()
        ) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };

        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportGetActionTests.class.getSimpleName()))
            .metadata(
                new Metadata.Builder().put(
                    new IndexMetadata.Builder(index1.getName()).settings(
                        Settings.builder()
                            .put("index.version.created", Version.CURRENT)
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    )
                )
            )
            .build();

        final ClusterState clusterState2 = ClusterState.builder(new ClusterName(TransportGetActionTests.class.getSimpleName()))
            .metadata(
                new Metadata.Builder().put(
                    new IndexMetadata.Builder(index2.getName()).settings(
                        Settings.builder()
                            .put("index.version.created", Version.CURRENT)
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index2.getUUID())
                    )
                )
            )
            .build();

        final ShardIterator index1ShardIterator = mock(ShardIterator.class);
        when(index1ShardIterator.shardId()).thenReturn(new ShardId(index1, randomInt()));

        final ShardIterator index2ShardIterator = mock(ShardIterator.class);
        when(index2ShardIterator.shardId()).thenReturn(new ShardId(index2, randomInt()));

        final OperationRouting operationRouting = mock(OperationRouting.class);
        when(
            operationRouting.getShards(eq(clusterState), eq(index1.getName()), anyString(), nullable(String.class), nullable(String.class))
        ).thenReturn(index1ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index1.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index1, randomInt())
        );

        final OperationRouting operationRouting2 = mock(OperationRouting.class);
        when(
            operationRouting2.getShards(
                eq(clusterState2),
                eq(index2.getName()),
                anyString(),
                nullable(String.class),
                nullable(String.class)
            )
        ).thenReturn(index2ShardIterator);
        when(operationRouting2.shardId(eq(clusterState2), eq(index2.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index2, randomInt())
        );

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        clusterService2 = mock(ClusterService.class);
        when(clusterService2.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService2.state()).thenReturn(clusterState2);
        when(clusterService2.operationRouting()).thenReturn(operationRouting2);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        transportService = null;
        clusterService = null;
        transportAction = null;
    }

    public void testIsPrimaryBasedRouting() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

        // request with preference set
        final GetRequestBuilder request = new GetRequestBuilder(client, GetAction.INSTANCE);
        request.setPreference(Preference.REPLICA.type()).setIndex("index1");

        transportAction = new TransportGetAction(
            clusterService,
            transportService,
            mock(IndicesService.class),
            threadPool,
            new ActionFilters(emptySet()),
            new Resolver()
        );

        // should return false since preference is set for request
        assertFalse(transportAction.isPrimaryBasedRouting(clusterService.state(), request.request(), "index1"));

        request.setPreference(null).setRealtime(false);
        // should return false since request is not realtime
        assertFalse(transportAction.isPrimaryBasedRouting(clusterService.state(), request.request(), "index1"));

        request.setRealtime(true);
        // should return false since segment replication is enabled
        assertTrue(transportAction.isPrimaryBasedRouting(clusterService.state(), request.request(), "index1"));

        // get request with document replication
        final GetRequestBuilder request2 = new GetRequestBuilder(client, GetAction.INSTANCE).setIndex("index2");

        transportAction = new TransportGetAction(
            clusterService2,
            transportService,
            mock(IndicesService.class),
            threadPool,
            new ActionFilters(emptySet()),
            new Resolver()
        );

        // should fail since document replication enabled
        assertFalse(transportAction.isPrimaryBasedRouting(clusterService2.state(), request2.request(), "index2"));

    }

    private static Task createTask() {
        return new Task(
            randomLong(),
            "transport",
            GetAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
    }

    static class Resolver extends IndexNameExpressionResolver {

        Resolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
            return new Index("index1", randomBase64UUID());
        }
    }

}
