/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.selector;


import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.promql.selector.LabelMatcher.Matcher.NEQ;
import static org.elasticsearch.xpack.esql.expression.promql.selector.LabelMatcher.Matcher.NREG;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EMPTY;

public class LabelMatcher {

    public static final String NAME = "__name__";

    public enum Matcher {
        EQ("="),
        NEQ("!="),
        REG("=~"),
        NREG("!~");

        public static Matcher from(String value) {
            switch (value) {
                case "=":
                    return EQ;
                case "!=":
                    return NEQ;
                case "=~":
                    return REG;
                case "!~":
                    return NREG;
                default:
                    return null;
            }
        }

        private final String value;

        Matcher(String value) {
            this.value = value;
        }
    }

    private final String name;
    private final String value;
    private final Matcher matcher;

    private final Automaton automaton;

    public LabelMatcher(String name, String value, Matcher matcher) {
        this.name = name;
        this.value = value;
        this.matcher = matcher;
        this.automaton = automaton(value, matcher);
    }

    public String name() {
        return name;
    }

    public Automaton automaton() {
        return automaton;
    }

    // TODO: externalize this to allow pluggable strategies (such as caching across labels/requests)
    private static Automaton automaton(String value, Matcher matcher) {
        Automaton automaton;
        // exact match
        if (matcher == Matcher.EQ || matcher == Matcher.NEQ) {
            automaton = Automata.makeString(value);
        }
        // regex match
        else {
            try {
                automaton = new RegExp(value).toAutomaton();
                automaton = MinimizationOperations.minimize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            } catch (IllegalArgumentException ex) {
                throw new QlIllegalArgumentException(ex, "Cannot parse regex {}", value);
            }
        }
        // negate if needed
        if (matcher == NEQ || matcher == NREG) {
            automaton = Operations.complement(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        return automaton;
    }

    public boolean matchesAll() {
        return Operations.isTotal(automaton);
    }

    public boolean matchesNone() {
        return Operations.isEmpty(automaton);
    }

    public boolean matchesEmpty() {
        return Operations.run(automaton, EMPTY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LabelMatcher label = (LabelMatcher) o;
        return matcher == label.matcher
            && Objects.equals(name, label.name)
            && Objects.equals(value, label.value)
            && Objects.equals(automaton, label.automaton);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, matcher, automaton);
    }

    @Override
    public String toString() {
        return name + matcher.value + value;
    }
}
