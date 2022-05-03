/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.OpenSearchException;

public interface ReplicationListener {

    void onDone(ReplicationState state);

    void onFailure(ReplicationState state, OpenSearchException e, boolean sendShardFailure);
}
