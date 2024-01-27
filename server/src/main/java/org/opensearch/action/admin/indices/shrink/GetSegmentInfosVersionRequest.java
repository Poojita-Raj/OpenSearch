/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.SegmentReplicationTransportRequest;

import java.io.IOException;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class GetSegmentInfosVersionRequest extends SegmentReplicationTransportRequest {

    private final ShardId primaryShardId;

    public GetSegmentInfosVersionRequest(StreamInput in) throws IOException {
        super(in);
        primaryShardId = new ShardId(in);
    }

    public GetSegmentInfosVersionRequest(
        String targetAllocationId,
        ShardId primaryShardId,
        DiscoveryNode targetNode
    ) {
        super(NO_OPS_PERFORMED , targetAllocationId, targetNode);
        this.primaryShardId = primaryShardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        primaryShardId.writeTo(out);
    }

    public ShardId getPrimaryShardId() {
        return primaryShardId;
    }
}
