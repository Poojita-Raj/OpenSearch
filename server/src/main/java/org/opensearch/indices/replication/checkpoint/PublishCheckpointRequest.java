/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;

import java.io.IOException;

public class PublishCheckpointRequest extends BroadcastRequest<PublishCheckpointRequest> {

    private ReplicationCheckpoint checkpoint;

    public PublishCheckpointRequest(ReplicationCheckpoint checkpoint, String... indices) {
        super(indices);
        this.checkpoint = checkpoint;
    }

    public PublishCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        this.checkpoint = new ReplicationCheckpoint(in);
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        checkpoint.writeTo(out);
    }

    @Override
    public String toString() {
        return "PublishCheckpointRequest{" +
            "checkpoint=" + checkpoint +
            '}';
    }
}
