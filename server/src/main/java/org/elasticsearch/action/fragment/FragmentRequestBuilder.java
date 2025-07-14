/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fragment;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

/**
 * A fragment document request builder.
 */
public class FragmentRequestBuilder extends ReplicationRequestBuilder<FragmentRequest, DocWriteResponse, FragmentRequestBuilder>
    implements
        WriteRequestBuilder<FragmentRequestBuilder> {

    private static final ActionType<DocWriteResponse> TYPE = new ActionType<>("indices:data/write/fragment");

    private String id = null;
    private BytesReference sourceBytesReference;
    private XContentType sourceContentType;
    private String routing;
    private WriteRequest.RefreshPolicy refreshPolicy;

    public FragmentRequestBuilder(ElasticsearchClient client) {
        this(client, null);
    }

    @SuppressWarnings("this-escape")
    public FragmentRequestBuilder(ElasticsearchClient client, @Nullable String index) {
        super(client, TYPE);
        setIndex(index);
    }

    /**
     * Sets the id of the fragment.
     */
    public FragmentRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public FragmentRequestBuilder setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the source.
     */
    public FragmentRequestBuilder setSource(BytesReference source, XContentType xContentType) {
        this.sourceBytesReference = source;
        this.sourceContentType = xContentType;
        return this;
    }

    public FragmentRequestBuilder setSource(XContentBuilder sourceBuilder) {
        this.sourceBytesReference = BytesReference.bytes(sourceBuilder);
        this.sourceContentType = sourceBuilder.contentType();
        return this;
    }

    @Override
    public FragmentRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public FragmentRequestBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refreshPolicy);
        return this;
    }

    @Override
    public FragmentRequest request() {
        FragmentRequest request = new FragmentRequest();
        super.apply(request);
        request.id(id);
        if (sourceBytesReference != null && sourceContentType != null) {
            request.source(sourceBytesReference, sourceContentType);
        }
        if (routing != null) {
            request.routing(routing);
        }
        if (refreshPolicy != null) {
            request.setRefreshPolicy(refreshPolicy);
        }
        return request;
    }
}
