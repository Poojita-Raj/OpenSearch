/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.action.StepListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;
import org.opensearch.transport.TransportService;

import java.util.List;

/**
 * The source for replication where the source is the primary shard of a cluster.
 */
public class PrimaryShardReplicationSource {

    public PrimaryShardReplicationSource(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        RecoverySettings recoverySettings,
        SegmentReplicationReplicaService segmentReplicationReplicaShardService
    ) {

    }

    public void getCheckpointInfo(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        StepListener<TransportCheckpointInfoResponse> listener
    ) {

    }

    public void getFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetFilesResponse> listener
    ) {

    }
}
