/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;

/**
 * Engine Factory implementation used with Segment Replication that wires up replica shards with an ${@link NRTReplicationEngine}
 * and primary with an ${@link InternalEngine}
 *
 * @opensearch.internal
 */
public class NRTReplicationEngineFactory implements EngineFactory {

    private static final Logger logger = LogManager.getLogger(NRTReplicationEngineFactory.class);
    private final ClusterService clusterService;

    public NRTReplicationEngineFactory(ClusterService clusterService) {
        logger.info("nrt factory created\n");
        this.clusterService = clusterService;
    }

    public NRTReplicationEngineFactory() {
        this.clusterService = null;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        logger.info("new read write engine entered\n");
        if (config.isReadOnlyReplica()) {
            logger.info("config: is read only replica\n");
            return new NRTReplicationEngine(config);
        }
        if (clusterService != null) {
            DiscoveryNodes nodes = this.clusterService.state().nodes();
            logger.info("min and max node version = {}, {}", nodes.getMinNodeVersion(), nodes.getMaxNodeVersion());
            //if (nodes.getMinNodeVersion() != nodes.getMaxNodeVersion()) {
                logger.info("min and max node version don't match, min version = {}", nodes.getMinNodeVersion());
                config.setClusterMinVersion(nodes.getMinNodeVersion());
            //}
        }
        return new InternalEngine(config);
    }
}
