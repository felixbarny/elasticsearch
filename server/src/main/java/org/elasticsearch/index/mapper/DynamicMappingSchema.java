/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public enum DynamicMappingSchema {
    ECS {
        private static final Map<String, Object> schema;

        static {
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, DynamicMappingSchema.class.getResourceAsStream("/schema/ecs.json"))
            ) {
                schema = parser.map();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public Map<String, Object> mappingForPath(String path) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapping = (Map<String, Object>) schema.get(path);
            if (mapping != null) {
                if (path.endsWith("message")) {
                    System.out.println(mapping);
                }
                return deepCopy(mapping);
            } else {
                return null;
            }
        }

        private static Map<String, Object> deepCopy(Map<String, Object> mapping) {
            return Maps.copyOf(mapping, v -> {
                if (v instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> m = (Map<String, Object>) v;
                    return new HashMap<>(deepCopy(m));
                } else {
                    return v;
                }
            });
        }

        @Override
        public String toString() {
            return "ecs";
        }
    };

    public abstract Map<String, Object> mappingForPath(String path);

    public static DynamicMappingSchema fromString(String value) {
        for (DynamicMappingSchema v : values()) {
            if (v.toString().equals(value)) {
                return v;
            }
        }
        throw new IllegalArgumentException("No matching schema for [" + value + "]");
    }

}
