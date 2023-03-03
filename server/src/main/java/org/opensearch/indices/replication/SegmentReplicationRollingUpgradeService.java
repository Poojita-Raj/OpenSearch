/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriterConfig;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.IndexService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.List;

public class SegmentReplicationRollingUpgradeService implements Releasable {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationRollingUpgradeService.class);

    private final ClusterApplierService clusterApplierService;
    private final SegmentReplicationRollingUpgradeListener clusterStateListener;

    public SegmentReplicationRollingUpgradeService(IndicesService indicesService, ClusterApplierService clusterApplierService) {
        logger.info("SEGREP ROLLING UPGRADE SERVICE REGISTERED!\n\n");
        SegmentReplicationRollingUpgradeListener clusterStateListener = new SegmentReplicationRollingUpgradeListener(indicesService);
        this.clusterApplierService = clusterApplierService;
        this.clusterStateListener = clusterStateListener;
        this.clusterApplierService.addListener(this.clusterStateListener);
    }

    @Override
    public void close() {
        this.clusterApplierService.removeListener(this.clusterStateListener);
    }
}
