/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

public class GetSegmentInfosVersionResponse extends TransportResponse {

    private final long primarySegInfosVersion;

    public GetSegmentInfosVersionResponse(final long primarySegInfosVersion) {
        this.primarySegInfosVersion = primarySegInfosVersion;
    }

    public GetSegmentInfosVersionResponse(StreamInput in) throws IOException {
        this.primarySegInfosVersion = in.readLong();
    }
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(primarySegInfosVersion);
    }

    public long getPrimarySegInfosVersion() {
        return primarySegInfosVersion;
    }
}
