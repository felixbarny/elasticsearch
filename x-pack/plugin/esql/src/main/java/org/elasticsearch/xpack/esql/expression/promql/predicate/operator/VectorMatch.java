/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.predicate.operator;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;

public class VectorMatch {

    public enum Filter {
        IGNORING,
        ON,
        NONE
    }

    public enum Grouping {
        LEFT,
        RIGHT,
        NONE
    }

    public static final VectorMatch NONE = new VectorMatch(Filter.NONE, emptySet(), Grouping.NONE, emptySet());

    private final Filter filter;
    private final Set<String> filterLabels;

    private final Grouping grouping;
    private final Set<String> groupingLabels;

    public VectorMatch(Filter filter, Set<String> filterLabels, Grouping grouping, Set<String> groupingLabels) {
        this.filter = filter;
        this.filterLabels = filterLabels;
        this.grouping = grouping;
        this.groupingLabels = groupingLabels;
    }

    public Filter filter() {
        return filter;
    }

    public Set<String> filterLabels() {
        return filterLabels;
    }

    public Grouping grouping() {
        return grouping;
    }

    public Set<String> groupingLabels() {
        return groupingLabels;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            VectorMatch that = (VectorMatch) o;
            return filter == that.filter
                && Objects.equals(filterLabels, that.filterLabels)
                && grouping == that.grouping
                && Objects.equals(groupingLabels, that.groupingLabels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, filterLabels, grouping, groupingLabels);
    }

    @Override
    public String toString() {
        String filterString = filter != Filter.NONE ? filter.name().toLowerCase(Locale.ROOT) + "(" + filterLabels + ")" : EMPTY;
        String groupingString = grouping != Grouping.NONE
            ? " "
            + grouping.name().toLowerCase(Locale.ROOT)
            + (groupingLabels.isEmpty() == false ? "(" + groupingLabels + ")" : EMPTY)
            + " "
            : EMPTY;
        return filterString + groupingString;
    }
}
