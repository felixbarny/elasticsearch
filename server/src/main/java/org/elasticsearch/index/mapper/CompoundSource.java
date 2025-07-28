/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A source that is a combination of multiple sources
 */
public class CompoundSource {
    private final XContentType combinedXContentType;
    private final List<XContentType> xContentTypes;
    private final List<BytesReference> sources;
    private BytesReference combinedSource;

    public CompoundSource(XContentType xContentType, BytesReference source) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(xContentType);
        this.combinedXContentType = xContentType;
        this.xContentTypes = List.of(xContentType);
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.sources = List.of(toBytesArray(source));
    }

    public CompoundSource(XContentType combinedXContentType, List<XContentType> xContentTypes, List<BytesReference> sources) {
        Objects.requireNonNull(sources);
        Objects.requireNonNull(combinedXContentType);
        Objects.requireNonNull(xContentTypes);
        if (xContentTypes.size() != sources.size()) {
            throw new IllegalArgumentException("xContentTypes and sources must have the same size");
        }
        this.combinedXContentType = combinedXContentType;
        this.xContentTypes = Collections.unmodifiableList(xContentTypes);
        this.sources = new ArrayList<>(sources.size());
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        for (int i = 0; i < sources.size(); i++) {
            this.sources.add(toBytesArray(sources.get(i)));
        }
    }

    private static BytesReference toBytesArray(BytesReference mainSource) {
        return mainSource.hasArray() ? mainSource : new BytesArray(mainSource.toBytesRef());
    }

    public BytesReference getMainSource() {
        return sources.isEmpty() ? null : sources.getFirst();
    }

    public List<BytesReference> getAdditionalSources() {
        if (sources.size() < 2) {
            return List.of();
        }
        return sources.subList(1, sources.size());
    }

    public List<BytesReference> getSources() {
        return sources;
    }

    public int estimateSize() {
        int length = 0;
        for (int i = 0; i < sources.size(); i++) {
            length += sources.get(i).length();
        }
        return length;
    }

    public boolean hasMultiple() {
        return sources.size() > 1;
    }

    public XContentType getXContentType() {
        return combinedXContentType;
    }

    public List<XContentType> getXContentTypes() {
        return xContentTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        CompoundSource that = (CompoundSource) o;
        return sources.equals(that.sources);
    }

    @Override
    public int hashCode() {
        return sources.hashCode();
    }

    public BytesReference combinedSource() {
        if (hasMultiple() == false) {
            return getMainSource();
        } else if (combinedSource != null) {
            return combinedSource;
        }

        // Create a builder with the same content type as the source
        try (XContentBuilder builder = XContentBuilder.builder(combinedXContentType.xContent())) {
            builder.startObject();

            // Keep track of fields we've already seen to avoid duplicates
            Set<String> processedFields = new HashSet<>();
            for (int i = 0; i < sources.size(); i++) {
                copyObjectFields(builder, xContentTypes.get(i), sources.get(i), processedFields);
            }
            builder.endObject();
            this.combinedSource = BytesReference.bytes(builder);
            return combinedSource;
        } catch (XContentParseException e) {
            throw new DocumentParsingException(e.getLocation(), e.getMessage(), e);
        } catch (IOException e) {
            // IOException from jackson, we don't have any useful location information here
            throw new DocumentParsingException(XContentLocation.UNKNOWN, "Error parsing document", e);
        }
    }

    private void copyObjectFields(XContentBuilder builder, XContentType xContentType, BytesReference source, Set<String> processedFields)
        throws IOException {
        StreamInput is = source.streamInput();
        try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, is)) {
            XContentParser.Token startObject = parser.nextToken();
            if (startObject != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(parser.getTokenLocation(), "Expected [START_OBJECT] but got [" + startObject + "]");
            }
            XContentParser.Token firstField = parser.nextToken();
            if (firstField != XContentParser.Token.FIELD_NAME) {
                throw new XContentParseException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + firstField + "]");
            }

            // Copy all fields from the source
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken(); // Move to the field value

                // Only add the field if we haven't seen it before
                if (processedFields.contains(fieldName) == false) {
                    processedFields.add(fieldName);
                    builder.field(fieldName);
                    builder.copyCurrentStructure(parser);
                } else {
                    // Skip this field as it's already been processed
                    parser.skipChildren();
                }

                parser.nextToken(); // Move to the next field name or END_OBJECT
            }
        }
    }
}
