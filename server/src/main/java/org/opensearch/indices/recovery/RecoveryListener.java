/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.OpenSearchException;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.common.ShardTargetListener;
import org.opensearch.indices.common.ShardTargetState;

/**
 * Listener that runs on changes in Recovery state
 *
 * @opensearch.internal
 */
public class RecoveryListener implements ShardTargetListener {

    /**
     * ShardRouting with which the shard was created
     */
    private final ShardRouting shardRouting;

    /**
     * Primary term with which the shard was created
     */
    private final long primaryTerm;

    private final IndicesClusterStateService indicesClusterStateService;

    public RecoveryListener(
        final ShardRouting shardRouting,
        final long primaryTerm,
        IndicesClusterStateService indicesClusterStateService
    ) {
        this.shardRouting = shardRouting;
        this.primaryTerm = primaryTerm;
        this.indicesClusterStateService = indicesClusterStateService;
    }

    @Override
    public void onDone(ShardTargetState state) {
        indicesClusterStateService.handleRecoveryDone(state, shardRouting, primaryTerm);
    }

    @Override
    public void onFailure(ShardTargetState state, OpenSearchException e, boolean sendShardFailure) {
        indicesClusterStateService.handleRecoveryFailure(shardRouting, sendShardFailure, e);
    }
}