/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.SegmentReplicationTargetService;

import java.util.Objects;

public class SegmentInfosVersionChecker {
    private final VersionChecker versionChecker;

    @Inject
    public SegmentInfosVersionChecker(SegmentReplicationTargetService targetService) {
        this(targetService::checkSegmentInfosVersionUpdated);
    }

    public SegmentInfosVersionChecker(VersionChecker versionChecker) {
        this.versionChecker = Objects.requireNonNull(versionChecker);
    }

    public boolean checkSegmentInfosVersionUpdated(IndexShard indexShard) {
        System.out.print("version checker called");
        return this.versionChecker.checkSegmentInfosVersionUpdated(indexShard);
    }

    public interface VersionChecker {
        public boolean checkSegmentInfosVersionUpdated(IndexShard indexShard);

        public static final SegmentInfosVersionChecker EMPTY = new SegmentInfosVersionChecker((indexShard) -> { return false; });
    }
}
