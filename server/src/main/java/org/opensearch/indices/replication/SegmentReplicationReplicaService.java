/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.OpenSearchException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.copy.ReplicationFailedException;
import org.opensearch.indices.replication.copy.SegmentReplicationState;

/**
 * Orchestrator of replication events.
 */
public class SegmentReplicationReplicaService {
    public interface SegmentReplicationListener extends ReplicationListener {

        @Override
        default void onDone(ReplicationState state) {
            onReplicationDone((SegmentReplicationState) state);
        }

        @Override
        default void onFailure(ReplicationState state, OpenSearchException e, boolean sendShardFailure) {
            onReplicationFailure((SegmentReplicationState) state, (ReplicationFailedException) e, sendShardFailure);
        }

        void onReplicationDone(SegmentReplicationState state);

        void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure);
    }
}
