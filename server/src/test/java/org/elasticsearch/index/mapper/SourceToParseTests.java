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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SourceToParseTests extends ESTestCase {

    public void testCombinedSourceWithNoFragments() throws IOException {
        BytesReference source = new BytesArray("{\"field1\":\"value1\",\"field2\":\"value2\"}");
        SourceToParse sourceToParse = new SourceToParse("1", source, XContentType.JSON);

        BytesReference combinedSource = sourceToParse.combinedSource();
        assertEquals(source, combinedSource);
    }

    public void testCombinedSourceWithFragments() throws IOException {
        // Create main source
        BytesReference mainSource = new BytesArray("{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Create fragments
        List<ParsedDocument> fragments = new ArrayList<>();

        // Fragment 1
        BytesReference fragment1Source = new BytesArray("{\"field3\":\"value3\",\"field4\":\"value4\"}");
        ParsedDocument fragment1 = createParsedDocument("1", fragment1Source, XContentType.JSON);
        fragments.add(fragment1);

        // Fragment 2 with overlapping field (should be ignored since main source has priority)
        BytesReference fragment2Source = new BytesArray("{\"field2\":\"updated\",\"field5\":\"value5\"}");
        ParsedDocument fragment2 = createParsedDocument("1", fragment2Source, XContentType.JSON);
        fragments.add(fragment2);

        // Create SourceToParse with fragments
        SourceToParse sourceToParse = SourceToParse.withFragments(
            "1",
            mainSource,
            XContentType.JSON,
            null,
            Map.of(),
            true,
            XContentMeteringParserDecorator.NOOP,
            fragments
        );

        // Get combined source
        BytesReference combinedSource = sourceToParse.combinedSource();

        // Verify the combined source
        Map<String, Object> combinedMap = XContentHelper.convertToMap(combinedSource, true, XContentType.JSON).v2();

        // Should have all unique fields
        assertEquals(5, combinedMap.size());
        assertEquals("value1", combinedMap.get("field1"));
        assertEquals("value2", combinedMap.get("field2")); // Main source's value should be kept
        assertEquals("value3", combinedMap.get("field3"));
        assertEquals("value4", combinedMap.get("field4"));
        assertEquals("value5", combinedMap.get("field5"));
    }

    public void testCombinedSourceWithNestedObjects() throws IOException {
        // Create main source with nested object
        BytesReference mainSource = new BytesArray("{\"field1\":\"value1\",\"nested\":{\"a\":1,\"b\":2}}");

        // Create fragments
        List<ParsedDocument> fragments = new ArrayList<>();

        // Fragment with nested object that should be merged
        BytesReference fragmentSource = new BytesArray("{\"nested\":{\"b\":3,\"c\":4},\"field2\":\"value2\"}");
        ParsedDocument fragment = createParsedDocument("1", fragmentSource, XContentType.JSON);
        fragments.add(fragment);

        // Create SourceToParse with fragments
        SourceToParse sourceToParse = SourceToParse.withFragments(
            "1",
            mainSource,
            XContentType.JSON,
            null,
            Map.of(),
            true,
            XContentMeteringParserDecorator.NOOP,
            fragments
        );

        // Get combined source
        BytesReference combinedSource = sourceToParse.combinedSource();

        // Verify the combined source
        Map<String, Object> combinedMap = XContentHelper.convertToMap(combinedSource, true, XContentType.JSON).v2();

        // Should have all top-level fields
        assertEquals(3, combinedMap.size());
        assertEquals("value1", combinedMap.get("field1"));
        assertEquals("value2", combinedMap.get("field2"));

        // Check nested object - the main source's nested object should completely replace the fragment's nested object
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedMap = (Map<String, Object>) combinedMap.get("nested");
        assertEquals(2, nestedMap.size());
        assertEquals(1, nestedMap.get("a")); // From main source
        assertEquals(2, nestedMap.get("b")); // From main source, not overridden by fragment
        assertFalse(nestedMap.containsKey("c")); // 'c' from fragment should not be present
    }

    public void testCombinedSourceWithDifferentContentTypes() throws IOException {
        // Create main source as JSON
        BytesReference mainSource = new BytesArray("{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Create fragments
        List<ParsedDocument> fragments = new ArrayList<>();

        // Fragment as CBOR
        BytesReference cborSource;
        try (var builder = XContentFactory.cborBuilder()) {
            builder.startObject();
            builder.field("field3", "value3");
            builder.field("field4", "value4");
            builder.endObject();
            cborSource = BytesReference.bytes(builder);
        }

        ParsedDocument fragment = createParsedDocument("1", cborSource, XContentType.CBOR);
        fragments.add(fragment);

        // Create SourceToParse with fragments
        SourceToParse sourceToParse = SourceToParse.withFragments(
            "1",
            mainSource,
            XContentType.JSON,
            null,
            Map.of(),
            true,
            XContentMeteringParserDecorator.NOOP,
            fragments
        );

        // Get combined source
        BytesReference combinedSource = sourceToParse.combinedSource();

        // Verify the combined source
        Map<String, Object> combinedMap = XContentHelper.convertToMap(combinedSource, true, XContentType.JSON).v2();

        // Should have all fields
        assertEquals(4, combinedMap.size());
        assertEquals("value1", combinedMap.get("field1"));
        assertEquals("value2", combinedMap.get("field2"));
        assertEquals("value3", combinedMap.get("field3"));
        assertEquals("value4", combinedMap.get("field4"));
    }

    private ParsedDocument createParsedDocument(String id, BytesReference source, XContentType xContentType) {
        LuceneDocument document = new LuceneDocument();
        document.add(IdFieldMapper.standardIdField(id));

        return new ParsedDocument(
            null,
            null,
            id,
            null,
            List.of(document),
            source,
            xContentType,
            null,
            XContentMeteringParserDecorator.UNKNOWN_SIZE,
            RoutingFields.Noop.INSTANCE
        );
    }
}
