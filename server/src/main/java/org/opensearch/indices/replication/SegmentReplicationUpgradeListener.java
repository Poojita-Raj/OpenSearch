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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.List;

public class SegmentReplicationUpgradeListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationUpgradeListener.class);

    private final IndicesService indicesService;

    public SegmentReplicationUpgradeListener(IndicesService indicesService) {
        logger.info("SEGREP ROLLING UPGRADE LISTENER REGISTERED!\n\n");
        this.indicesService = indicesService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        logger.info("rolling upgrade listener called on clusterChangedEvent\n\n\n");
        List<IndexShard> indexShardList = new ArrayList<>();
        DiscoveryNodes nodes = event.state().nodes();
        if (nodes.getMinNodeVersion().equals(nodes.getMaxNodeVersion())) {
            logger.info("cluster all nodes version are equal\n");
            for (IndexService sh : indicesService.indices().values()) {
                for (IndexShard is : sh) {
                    if (is.getEngine().config().getClusterBwcVersion() != null) {
                        // close indexservice
                        // reopen index writer
                        indexShardList.add(is);
                        logger.info("added index shard to update iw: {}", is.toString());
                    }
                }
            }
        }
        try {
            if (indexShardList.size() > 0) {
                for (IndexShard is : indexShardList) {
                    is.resetEngineToGlobalCheckpoint();
                }
            }
        } catch (Exception e) {
            logger.info("got error here: {} {}", e.toString(), e.getMessage());
            e.printStackTrace();
        }

    }

}
