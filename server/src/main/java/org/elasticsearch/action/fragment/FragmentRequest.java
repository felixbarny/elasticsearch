/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fragment;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * A document fragment represents a reusable portion of document content that can be referenced
 * by other {@link org.elasticsearch.action.index.IndexRequest}s in a bulk operation to reduce
 * redundant parsing and processing of identical fields.
 */
public class FragmentRequest extends ReplicatedWriteRequest<FragmentRequest> implements DocWriteRequest<FragmentRequest> {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FragmentRequest.class);
    private static final ShardId NO_SHARD_ID = null;

    private String id;
    private BytesReference source;
    private XContentType contentType;
    private IndexRouting.ExtractFromSource.Builder routingBuilder;

    public FragmentRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        this.id = in.readString();
        this.source = in.readBytesReference();
        boolean hasContentType = in.readBoolean();
        if (hasContentType) {
            this.contentType = XContentType.ofOrdinal(in.readByte());
        }
    }

    public FragmentRequest() {
        super(NO_SHARD_ID);
    }

    /**
     * Constructs a new fragment request against the specified index and id.
     */
    public FragmentRequest(String index, String id) {
        super(NO_SHARD_ID);
        this.index = index;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        if (contentType == null) {
            validationException = addValidationError("content type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("fragment id is required", validationException);
        }
        return validationException;
    }

    /**
     * The id of the document fragment.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the document fragment.
     */
    public FragmentRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override
    public FragmentRequest routing(String routing) {
        throw new UnsupportedOperationException("Fragment requests do not support routing");
    }

    @Override
    public String routing() {
        return null;
    }

    /**
     * The source of the document fragment, recopied to a new array if it is unsafe.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * Index the Map in {@link Requests#INDEX_CONTENT_TYPE} format
     *
     * @param source The map to index
     */
    public FragmentRequest source(Map<String, ?> source) throws ElasticsearchGenerationException {
        return source(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public FragmentRequest source(Map<String, ?> source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the content source to index.
     */
    public FragmentRequest source(XContentBuilder sourceBuilder) {
        return source(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType());
    }

    /**
     * Sets the document fragment to index in bytes form.
     */
    public FragmentRequest source(BytesReference source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.contentType = Objects.requireNonNull(xContentType);
        return this;
    }

    /**
     * The content type of the fragment source.
     */
    public XContentType getContentType() {
        return contentType;
    }

    @Override
    public OpType opType() {
        return OpType.FRAGMENT;
    }

    @Override
    public boolean isRequireAlias() {
        return false;
    }

    @Override
    public boolean isRequireDataStream() {
        return false;
    }

    @Override
    public FragmentRequest setIfSeqNo(long seqNo) {
        throw new UnsupportedOperationException("Fragment requests do not support sequence numbers");
    }

    @Override
    public FragmentRequest setIfPrimaryTerm(long term) {
        throw new UnsupportedOperationException("Fragment requests do not support primary terms");
    }

    @Override
    public long ifSeqNo() {
        return UNASSIGNED_SEQ_NO;
    }

    @Override
    public long ifPrimaryTerm() {
        return UNASSIGNED_PRIMARY_TERM;
    }

    @Override
    public FragmentRequest version(long version) {
        throw new UnsupportedOperationException("Fragment requests do not support versioning");
    }

    @Override
    public long version() {
        return Versions.MATCH_ANY;
    }

    @Override
    public FragmentRequest versionType(VersionType versionType) {
        throw new UnsupportedOperationException("Fragment requests do not support version types");
    }

    @Override
    public VersionType versionType() {
        return VersionType.INTERNAL;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeBody(out);
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        writeBody(out);
    }

    private void writeBody(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBytesReference(source);
        out.writeBoolean(contentType != null);
        if (contentType != null) {
            XContentHelper.writeTo(out, contentType);
        }
    }

    @Override
    public int route(IndexRouting indexRouting) {
        throw new UnsupportedOperationException("Fragment requests do not support routing");
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            if (source.length() > IndexRequest.MAX_SOURCE_LENGTH_IN_TOSTRING) {
                sSource = "n/a, actual length: ["
                    + ByteSizeValue.ofBytes(source.length()).toString()
                    + "], max length: "
                    + ByteSizeValue.ofBytes(IndexRequest.MAX_SOURCE_LENGTH_IN_TOSTRING).toString();
            } else {
                sSource = XContentHelper.convertToJson(source, false);
            }
        } catch (Exception e) {
            // ignore
        }
        return "fragment {[" + index + "][" + id + "], source[" + sSource + "]}";
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(id) + (source == null ? 0 : source.length());
    }

    /**
     * Returns the routing builder for the document fragment.
     * Returns a cached value if the builder has already been created
     * to avoid re-computing it if this fragment is referenced multiple times.
     *
     * @param hashSource A function that returns the routing builder for the document fragment.
     * @return The routing builder for the document fragment.
     */
    public IndexRouting.ExtractFromSource.Builder getRoutingBuilder(
        BiFunction<XContentType, BytesReference, IndexRouting.ExtractFromSource.Builder> hashSource
    ) {
        if (routingBuilder == null) {
            routingBuilder = hashSource.apply(contentType, source);
            // sorting the fragment routing builder speeds up the sorting of the merged routing builders
            routingBuilder.sort();
        }
        return routingBuilder;
    }

    public SourceToParse getSourceToParse(
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator
    ) {
        return new SourceToParse(
            id(),
            source(),
            getContentType(),
            routing(),
            dynamicTemplates,
            includeSourceOnError,
            meteringParserDecorator,
            true,
            List.of()
        );
    }
}
