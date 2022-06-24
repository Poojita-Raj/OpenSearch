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
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRecoveryException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationCollection.ReplicationRef;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service class that orchestrates replication events on replicas.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationTargetService.class);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    private final ReplicationCollection<SegmentReplicationTarget> onGoingReplications;

    private final SegmentReplicationSourceFactory sourceFactory;

    /**
     * The internal actions
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String FILE_CHUNK = "internal:index/shard/replication/file_chunk";
    }

    public SegmentReplicationTargetService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService,
        final SegmentReplicationSourceFactory sourceFactory
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.onGoingReplications = new ReplicationCollection<>(logger, threadPool);
        this.sourceFactory = sourceFactory;

        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            ThreadPool.Names.GENERIC,
            FileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingReplications.cancelForShard(shardId, "shard closed");
        }
    }

    public void prepareForReplication(
        IndexShard indexShard,
        DiscoveryNode targetNode,
        DiscoveryNode sourceNode,
        ActionListener<TrackShardResponse> listener
    ) {
        setupReplicaShard(indexShard);
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction retryableAction = new RetryableAction(logger, threadPool, initialDelay, timeout, listener) {
            @Override
            public void tryAction(ActionListener listener) {
                sourceFactory.getTransportService().sendRequest(
                    sourceNode,
                    SegmentReplicationSourceService.Actions.TRACK_SHARD,
                    new TrackShardRequest(indexShard.shardId(), indexShard.routingEntry().allocationId().getId(), targetNode),
                    TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                    new ActionListenerResponseHandler<>(listener, TrackShardResponse::new)
                );
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        retryableAction.run();
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException
                || cause instanceof OpenSearchRejectedExecutionException
                || cause instanceof DelayRecoveryException;
        }
        return false;
    }

    private void setupReplicaShard(IndexShard indexShard) throws IndexShardRecoveryException {
        indexShard.prepareForIndexRecovery();
        final Store store = indexShard.store();
        store.incRef();
        try {
            store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(
                indexShard.shardPath().resolveTranslog(),
                SequenceNumbers.NO_OPS_PERFORMED,
                indexShard.shardId(),
                indexShard.getPendingPrimaryTerm()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            indexShard.persistRetentionLeases();
            indexShard.openEngineAndSkipTranslogRecovery();
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(indexShard.shardId(), "failed to start replica shard", e);
        } finally {
            store.decRef();
        }
    }


    /**
     * Invoked when a new checkpoint is received from a primary shard.
     * It checks if a new checkpoint should be processed or not and starts replication if needed.
     * @param receivedCheckpoint       received checkpoint that is checked for processing
     * @param replicaShard      replica shard on which checkpoint is received
     */
    public synchronized void onNewCheckpoint(final ReplicationCheckpoint receivedCheckpoint, final IndexShard replicaShard) {
        if (onGoingReplications.isShardReplicating(replicaShard.shardId())) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Ignoring new replication checkpoint - shard is currently replicating to checkpoint {}",
                    replicaShard.getLatestReplicationCheckpoint()
                )
            );
            return;
        }
        if (replicaShard.shouldProcessCheckpoint(receivedCheckpoint)) {
            startReplication(receivedCheckpoint, replicaShard, new SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {}

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    if (sendShardFailure == true) {
                        logger.error("replication failure", e);
                        replicaShard.failShard("replication failure", e);
                    }
                }
            });

        }
    }

    public void startReplication(
        final ReplicationCheckpoint checkpoint,
        final IndexShard indexShard,
        final SegmentReplicationListener listener
    ) {
        startReplication(new SegmentReplicationTarget(checkpoint, indexShard, sourceFactory.get(indexShard), listener));
    }

    public void startReplication(final SegmentReplicationTarget target) {
        final long replicationId = onGoingReplications.start(target, recoverySettings.activityTimeout());
        logger.trace(() -> new ParameterizedMessage("Starting replication {}", replicationId));
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    /**
     * Listener that runs on changes in Replication state
     *
     * @opensearch.internal
     */
    public interface SegmentReplicationListener extends ReplicationListener {

        @Override
        default void onDone(ReplicationState state) {
            onReplicationDone((SegmentReplicationState) state);
        }

        @Override
        default void onFailure(ReplicationState state, OpenSearchException e, boolean sendShardFailure) {
            onReplicationFailure((SegmentReplicationState) state, e, sendShardFailure);
        }

        void onReplicationDone(SegmentReplicationState state);

        void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure);
    }

    /**
     * Start the recovery of a shard using Segment Replication.  This method will first setup the shard and then start segment copy.
     *
     * @param indexShard          {@link IndexShard} The target IndexShard.
     * @param targetNode          {@link DiscoveryNode} The IndexShard's DiscoveryNode
     * @param sourceNode          {@link DiscoveryNode} The source node.
     * @param replicationSource   {@link PrimaryShardReplicationSource} The source from where segments will be retrieved.
     * @param replicationListener {@link ReplicationListener} listener.
     */
    public void startRecovery(
        IndexShard indexShard,
        DiscoveryNode targetNode,
        DiscoveryNode sourceNode,
        PrimaryShardReplicationSource replicationSource,
        SegmentReplicationListener replicationListener
    ) {
        indexShard.markAsReplicating();
        StepListener<TrackShardResponse> trackShardListener = new StepListener<>();
        trackShardListener.whenComplete(
            r -> { startReplication(indexShard.getLatestReplicationCheckpoint(), indexShard, replicationSource, replicationListener); },
            e -> { replicationListener.onFailure(indexShard.getReplicationState(), new ReplicationFailedException(indexShard, e), true); }
        );
        prepareForReplication(indexShard, targetNode, sourceNode, trackShardListener);
    }

    /**
     * Runnable implementation to trigger a replication event.
     */
    private class ReplicationRunner implements Runnable {

        final long replicationId;

        public ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void run() {
            start(replicationId);
        }
    }

    private void start(final long replicationId) {
        try (ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.get(replicationId)) {
            replicationRef.get().startReplication(new ActionListener<>() {
                @Override
                public void onResponse(Void o) {
                    onGoingReplications.markAsDone(replicationId);
                }

                @Override
                public void onFailure(Exception e) {
                    onGoingReplications.fail(replicationId, new OpenSearchException("Segment Replication failed", e), true);
                }
            });
        }
    }

    private class FileChunkTransportRequestHandler implements TransportRequestHandler<FileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final FileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<SegmentReplicationTarget> ref = onGoingReplications.getSafe(request.recoveryId(), request.shardId())) {
                final SegmentReplicationTarget target = ref.get();
                final ActionListener<Void> listener = target.createOrFinishListener(channel, Actions.FILE_CHUNK, request);
                target.handleFileChunk(request, target, bytesSinceLastPause, recoverySettings.rateLimiter(), listener);
            }
        }
    }
}
