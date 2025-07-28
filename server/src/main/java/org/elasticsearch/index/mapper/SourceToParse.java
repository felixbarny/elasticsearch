/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SourceToParse {

    private final BytesReference source;

    private final String id;

    private final @Nullable String routing;

    private final XContentType xContentType;

    private final Map<String, String> dynamicTemplates;

    private final boolean includeSourceOnError;

    private final XContentMeteringParserDecorator meteringParserDecorator;

    private final boolean fragment;

    private final List<ParsedDocument> fragments;

    private BytesReference combinedSource;

    public static SourceToParse ofFragment(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator
    ) {
        return new SourceToParse(
            id,
            source,
            xContentType,
            routing,
            dynamicTemplates,
            includeSourceOnError,
            meteringParserDecorator,
            true,
            List.of()
        );
    }

    public static SourceToParse withFragments(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator,
        List<ParsedDocument> fragments
    ) {
        return new SourceToParse(
            id,
            source,
            xContentType,
            routing,
            dynamicTemplates,
            includeSourceOnError,
            meteringParserDecorator,
            false,
            fragments
        );
    }

    public SourceToParse(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator
    ) {
        this(id, source, xContentType, routing, dynamicTemplates, includeSourceOnError, meteringParserDecorator, false, List.of());
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, null, Map.of(), true, XContentMeteringParserDecorator.NOOP);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType, String routing) {
        this(id, source, xContentType, routing, Map.of(), true, XContentMeteringParserDecorator.NOOP);
    }

    private SourceToParse(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator,
        boolean fragment,
        List<ParsedDocument> fragments
    ) {
        if (fragment && fragments.isEmpty() == false) {
            throw new IllegalStateException("A fragment can't contain other fragments");
        }
        Objects.requireNonNull(fragments, "fragments must not be null");
        this.id = id;
        this.source = source;
        this.xContentType = xContentType;
        this.routing = routing;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
        this.includeSourceOnError = includeSourceOnError;
        this.meteringParserDecorator = meteringParserDecorator;
        this.fragment = fragment;
        this.fragments = fragments;
    }

    public SourceToParse(
        String id,
        BytesReference source,
        XContentType xContentType,
        String routing,
        Map<String, String> dynamicTemplates
    ) {
        this(id, source, xContentType, routing, dynamicTemplates, true, XContentMeteringParserDecorator.NOOP);
    }

    public BytesReference source() {
        return this.source;
    }

    public int estimateSourceSize() {
        int length = this.source.length();
        for (ParsedDocument fragment : fragments) {
            length += fragment.mainSource().length();
        }
        return length;
    }

    /**
     * The {@code _id} provided on the request or calculated on the
     * coordinating node. If the index is in {@code time_series} mode then
     * the coordinating node will not calculate the {@code _id}. In that
     * case this will be {@code null} if one isn't sent on the request.
     * <p>
     * Use {@link DocumentParserContext#documentDescription()} to generate
     * a description of the document for errors instead of calling this
     * method.
     */
    @Nullable
    public String id() {
        return this.id;
    }

    public @Nullable String routing() {
        return this.routing;
    }

    /**
     * Returns a map from the full path (i.e. foo.bar) of field names to the names of dynamic mapping templates.
     */
    public Map<String, String> dynamicTemplates() {
        return dynamicTemplates;
    }

    public XContentType getXContentType() {
        return this.xContentType;
    }

    public XContentMeteringParserDecorator getMeteringParserDecorator() {
        return meteringParserDecorator;
    }

    public boolean getIncludeSourceOnError() {
        return includeSourceOnError;
    }

    public boolean isFragment() {
        return fragment;
    }

    public List<ParsedDocument> getFragments() {
        return fragments;
    }

    /**
     * Combines the main source with the sources from all fragments.
     * If there are no fragments, returns the original source.
     * When fields appear in both the main source and fragments, the main source values take precedence.
     *
     * @return A BytesReference containing the combined source
     */
    public BytesReference combinedSource() {
        if (fragments.isEmpty()) {
            return source;
        } else if (combinedSource != null) {
            return combinedSource;
        }
        List<BytesReference> sources = new ArrayList<>(fragments.size() + 1);
        List<XContentType> xContentTypes = new ArrayList<>(fragments.size() + 1);
        sources.add(source);
        xContentTypes.add(xContentType);
        for (ParsedDocument fragment : fragments) {
            sources.addAll(fragment.source().getSources());
            xContentTypes.addAll(fragment.source().getXContentTypes());
        }
        combinedSource = new CompoundSource(xContentType, xContentTypes, sources).combinedSource();
        return combinedSource;
    }

}
