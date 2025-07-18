/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fragment;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class FragmentResponse extends DocWriteResponse {

    public FragmentResponse(String index, String id) {
        super(new ShardId(index, ClusterState.UNKNOWN_UUID, -1), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, 0, Result.FRAGMENT);
    }

    public FragmentResponse(ShardId shardId, String id) {
        super(shardId, id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, 0, Result.FRAGMENT);
    }

    public FragmentResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
    }

    public FragmentResponse(StreamInput in) throws IOException {
        super(in);
    }

}
