/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents the target of a replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTarget extends ReplicationTarget {

    private final ReplicationCheckpoint checkpoint;
    private final SegmentReplicationSource source;
    private final SegmentReplicationState state;
    protected final MultiFileWriter multiFileWriter;

    public SegmentReplicationTarget(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        SegmentReplicationSource source,
        SegmentReplicationTargetService.SegmentReplicationListener listener
    ) {
        super("replication_target", indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        this.state = new SegmentReplicationState(stateIndex);
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), stateIndex, getPrefix(), logger, this::ensureRefCount);
    }

    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
        }
    }

    @Override
    protected String getPrefix() {
        return "replication." + UUIDs.randomBase64UUID() + ".";
    }

    @Override
    protected void onDone() {
        state.setStage(SegmentReplicationState.Stage.DONE);
    }

    @Override
    public void onCancel(String reason) {
        state.setStage(SegmentReplicationState.Stage.DONE);
        cancellableThreads.cancel(reason);
    }

    @Override
    public SegmentReplicationState state() {
        return state;
    }

    @Override
    public ReplicationTarget retryCopy() {
        // TODO
        return null;
    }

    @Override
    public String description() {
        return "Segment replication from " + source.toString();
    }

    @Override
    public void notifyListener(OpenSearchException e, boolean sendShardFailure) {
        listener.onFailure(state(), e, sendShardFailure);
    }

    @Override
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        // TODO
        return false;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        try {
            multiFileWriter.writeFileChunk(metadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Start the Replication event.
     * @param listener {@link ActionListener} listener.
     */
    public void startReplication(ActionListener<ReplicationResponse> listener) {
        final StepListener<CheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetSegmentFilesResponse> getFilesListener = new StepListener<>();
        final StepListener<Void> finalizeListener = new StepListener<>();

        // Get list of files to copy from this checkpoint.
        source.getCheckpointMetadata(getId(), checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> getFiles(checkpointInfo, getFilesListener), listener::onFailure);
        getFilesListener.whenComplete(
            response -> finalizeReplication(checkpointInfoListener.result(), finalizeListener),
            listener::onFailure
        );
        finalizeListener.whenComplete(r -> listener.onResponse(new ReplicationResponse()), listener::onFailure);
    }

    private void getFiles(CheckpointInfoResponse checkpointInfo, StepListener<GetSegmentFilesResponse> getFilesListener)
        throws IOException {
        final Store.MetadataSnapshot snapshot = checkpointInfo.getSnapshot();
        Store.MetadataSnapshot localMetadata = getMetadataSnapshot();
        final Store.RecoveryDiff diff = snapshot.recoveryDiff(localMetadata);
        logger.debug("Recovery diff {}", diff);
        final List<StoreFileMetadata> filesToFetch = Stream.concat(diff.missing.stream(), diff.different.stream())
            .collect(Collectors.toList());

        Set<String> storeFiles = new HashSet<>(Arrays.asList(store.directory().listAll()));
        final Set<StoreFileMetadata> pendingDeleteFiles = checkpointInfo.getPendingDeleteFiles()
            .stream()
            .filter(f -> storeFiles.contains(f.name()) == false)
            .collect(Collectors.toSet());

        filesToFetch.addAll(pendingDeleteFiles);

        for (StoreFileMetadata file : filesToFetch) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        if (filesToFetch.isEmpty()) {
            getFilesListener.onResponse(new GetSegmentFilesResponse());
        }
        source.getSegmentFiles(getId(), checkpointInfo.getCheckpoint(), filesToFetch, store, getFilesListener);
    }

    private void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                // Deserialize the new SegmentInfos object sent from the primary.
                final ReplicationCheckpoint responseCheckpoint = checkpointInfoResponse.getCheckpoint();
                SegmentInfos infos = SegmentInfos.readCommit(
                    store.directory(),
                    toIndexInput(checkpointInfoResponse.getInfosBytes()),
                    responseCheckpoint.getSegmentsGen()
                );
                indexShard.finalizeReplication(infos, checkpointInfoResponse.getSnapshot(), responseCheckpoint.getSeqNo());
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                ReplicationFailedException rfe = new ReplicationFailedException(indexShard.shardId(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                ReplicationFailedException rfe = new ReplicationFailedException(indexShard.shardId(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    /**
     * This method formats our byte[] containing the primary's SegmentInfos into lucene's {@link ChecksumIndexInput} that can be
     * passed to SegmentInfos.readCommit
     */
    private ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(
            new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(input))), "SegmentInfos")
        );
    }

    private Store.MetadataSnapshot getMetadataSnapshot() throws IOException {
        if (indexShard.state().equals(IndexShardState.STARTED) == false) {
            return Store.MetadataSnapshot.EMPTY;
        }
        return store.getMetadata(indexShard.getLatestSegmentInfos());
    }
}
