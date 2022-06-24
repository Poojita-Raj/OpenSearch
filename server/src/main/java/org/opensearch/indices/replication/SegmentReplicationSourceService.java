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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Service class that handles segment replication requests from replica shards.
 * Typically, the "source" is a primary shard. This code executes on the source node.
 *
 * @opensearch.internal
 */
public final class SegmentReplicationSourceService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationSourceService.class);
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;
    private final IndicesService indicesService;

    /**
     * Internal actions used by the segment replication source service on the primary shard
     *
     * @opensearch.internal
     */
    public static class Actions {

        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/replication/get_checkpoint_info";
        public static final String GET_SEGMENT_FILES = "internal:index/shard/replication/get_segment_files";
        public static final String TRACK_SHARD = "internal:index/shard/segrep/track_shard";
    }

    private final OngoingSegmentReplications ongoingSegmentReplications;

    public SegmentReplicationSourceService(
        IndicesService indicesService,
        TransportService transportService,
        RecoverySettings recoverySettings
    ) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            CheckpointInfoRequest::new,
            new CheckpointInfoRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_SEGMENT_FILES,
            ThreadPool.Names.GENERIC,
            GetSegmentFilesRequest::new,
            new GetSegmentFilesRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.TRACK_SHARD,
            ThreadPool.Names.GENERIC,
            TrackShardRequest::new,
            new TrackShardRequestHandler()
        );
        this.ongoingSegmentReplications = new OngoingSegmentReplications(indicesService, recoverySettings);
    }

    private class CheckpointInfoRequestHandler implements TransportRequestHandler<CheckpointInfoRequest> {
        @Override
        public void messageReceived(CheckpointInfoRequest request, TransportChannel channel, Task task) throws Exception {
            final RemoteSegmentFileChunkWriter segmentSegmentFileChunkWriter = new RemoteSegmentFileChunkWriter(
                request.getReplicationId(),
                recoverySettings,
                new RetryableTransportClient(
                    transportService,
                    request.getTargetNode(),
                    recoverySettings.internalActionRetryTimeout(),
                    logger
                ),
                request.getCheckpoint().getShardId(),
                SegmentReplicationTargetService.Actions.FILE_CHUNK,
                new AtomicLong(0),
                (throttleTime) -> {}
            );
            final CopyState copyState = ongoingSegmentReplications.prepareForReplication(request, segmentSegmentFileChunkWriter);
            channel.sendResponse(
                new CheckpointInfoResponse(
                    copyState.getCheckpoint(),
                    copyState.getMetadataSnapshot(),
                    copyState.getInfosBytes(),
                    copyState.getPendingDeleteFiles()
                )
            );
        }
    }

    private class GetSegmentFilesRequestHandler implements TransportRequestHandler<GetSegmentFilesRequest> {
        @Override
        public void messageReceived(GetSegmentFilesRequest request, TransportChannel channel, Task task) throws Exception {
            ongoingSegmentReplications.startSegmentCopy(request, new ChannelActionListener<>(channel, Actions.GET_SEGMENT_FILES, request));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                ongoingSegmentReplications.cancelReplication(removedNode);
            }
        }
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ongoingSegmentReplications.cancel(indexShard, "shard is closed");
        }
    }

    class TrackShardRequestHandler implements TransportRequestHandler<TrackShardRequest> {
        @Override
        public void messageReceived(TrackShardRequest request, TransportChannel channel, Task task) throws Exception {
            final ShardId shardId = request.getShardId();
            final String targetAllocationId = request.getTargetAllocationId();

            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard shard = indexService.getShard(shardId.id());

            final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();

            if (routingTable.getByAllocationId(targetAllocationId) == null) {
                throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
            }
            final StepListener<ReplicationResponse> addRetentionLeaseStep = new StepListener<>();
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(this + "[onFailure]");
                logger.error(
                    new ParameterizedMessage(
                        "Error marking shard {} as tracked for allocation ID {}",
                        shardId,
                        request.getTargetAllocationId()
                    ),
                    e
                );
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on track shard request", inner);
                }
            };
            RunUnderPrimaryPermit.run(
                () -> shard.cloneLocalPeerRecoveryRetentionLease(
                    request.getTargetNode().getId(),
                    new ThreadedActionListener<>(logger, shard.getThreadPool(), ThreadPool.Names.GENERIC, addRetentionLeaseStep, false)
                ),
                "Add retention lease step",
                shard,
                new CancellableThreads(),
                logger
            );
            addRetentionLeaseStep.whenComplete(r -> {
                RunUnderPrimaryPermit.run(
                    () -> shard.initiateTracking(targetAllocationId),
                    shardId + " initiating tracking of " + targetAllocationId,
                    shard,
                    new CancellableThreads(),
                    logger
                );
                RunUnderPrimaryPermit.run(
                    () -> shard.updateLocalCheckpointForShard(targetAllocationId, SequenceNumbers.NO_OPS_PERFORMED),
                    shardId + " marking " + targetAllocationId + " as in sync",
                    shard,
                    new CancellableThreads(),
                    logger
                );
                channel.sendResponse(new TrackShardResponse());
            }, onFailure);
        }
    }
}
