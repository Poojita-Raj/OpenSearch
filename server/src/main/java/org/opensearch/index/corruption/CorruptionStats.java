/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.corruption;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class CorruptionStats implements ToXContentFragment, Writeable {
    private final AtomicInteger currentCorruptions = new AtomicInteger();

    public CorruptionStats() {}

    public CorruptionStats(StreamInput in) throws IOException {
        currentCorruptions.set(in.readVInt());
    }

    public void add(CorruptionStats corruptionStats) {
        if (corruptionStats != null) {
            this.currentCorruptions.addAndGet(corruptionStats.currentCorruptions());
        }
    }

    /**
     * Number of recorded data corruptions in this shard
     */
    public int currentCorruptions() {
        return currentCorruptions.get();
    }

    public void incCurrentCorruptions() {
        currentCorruptions.incrementAndGet();
    }

    public void resetCurrentCorruptions() {
        currentCorruptions.set(0);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CorruptionStats.Fields.CORRUPTION);
        builder.field(CorruptionStats.Fields.CURRENT_CORRUPTION, currentCorruptions());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String CORRUPTION = "corruption";
        static final String CURRENT_CORRUPTION = "current_corruption_count";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(currentCorruptions.get());
    }

    @Override
    public String toString() {
        return "corruptionStats, currentCorruptions ["
            + currentCorruptions()
            + "]";
    }
}
