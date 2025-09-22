/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.esql.expression.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ModifierFunctionResolution;
import org.elasticsearch.xpack.esql.expression.predicate.operator.aggregation.AggregationOperator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.aggregation.AggregationOperator.Grouping;
import org.elasticsearch.xpack.esql.expression.predicate.operator.aggregation.ParameterizedAggregationOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorMatch;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.arithmetic.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.arithmetic.VectorBinaryArithmetic.ArithmeticOp;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison.VectorBinaryComparison.ComparisonOp;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set.VectorBinarySet;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set.VectorBinarySet.SetOp;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;
import org.elasticsearch.xpack.esql.expression.selector.Evaluation;
import org.elasticsearch.xpack.esql.expression.selector.InstantSelector;
import org.elasticsearch.xpack.esql.expression.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.expression.selector.RangeSelector;
import org.elasticsearch.xpack.esql.expression.selector.Selector;
import org.elasticsearch.xpack.esql.expression.subquery.Subquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ArithmeticBinaryContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.FunctionContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.FunctionModifierContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.HexLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelListContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ModifierContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ParenthesizedContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SingleExpressionContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.StringContext;
import org.elasticsearch.xpack.esql.util.ParsingUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.promql.selector.LabelMatcher.Matcher;
import static org.elasticsearch.xpack.esql.expression.promql.selector.LabelMatcher.NAME;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AND;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ASTERISK;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AtContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.CARET;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DecimalLiteralContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DurationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EvaluationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelsContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.MINUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.NEQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OR;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OffsetContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PERCENT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PLUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SLASH;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SelectorContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SeriesMatcherContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SubqueryContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.UNLESS;

class ExpressionBuilder extends IdentifierBuilder {

    private final Instant start, stop;

    ExpressionBuilder() {
        this(null, null);
    }

    ExpressionBuilder(Instant start, Instant stop) {
        Instant now = null;
        if (start == null || stop == null) {
            now = DateUtils.nowWithMillisResolution().toInstant();
        }

        this.start = start != null ? start : now;
        this.stop = stop != null ? stop : now;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(SingleExpressionContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Source source = source(ctx);
        Expression expression = expression(ctx.expression());
        DataType dataType = expression.dataType();
        if ((PromqlDataTypes.isScalar(dataType) || PromqlDataTypes.isInstantVector(dataType)) == false) {
            throw new ParsingException(
                source,
                "Unary expression only allowed on expressions of type scalar or instance vector, got [{}]",
                dataType.typeName()
            );
        }
        // convert - into a binary operator
        if (ctx.operator.getType() == MINUS) {
            expression = new VectorBinaryArithmetic(source, Literal.fromDouble(source, 0.0), expression, VectorMatch.NONE, ArithmeticOp.SUB);
        }

        return expression;
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression le = expression(ctx.left);
        Expression re = expression(ctx.right);
        Source source = source(ctx);

        boolean bool = ctx.BOOL() != null;
        int opType = ctx.op.getType();
        String opText = ctx.op.getText();

        // validate operation against expression types
        boolean leftIsScalar = PromqlDataTypes.isScalar(le.dataType());
        boolean rightIsScalar = PromqlDataTypes.isScalar(re.dataType());

        // comparisons against scalars require bool
        if (bool == false && leftIsScalar && rightIsScalar) {
            switch (opType) {
                case EQ:
                case NEQ:
                case LT:
                case LTE:
                case GT:
                case GTE:
                    throw new ParsingException(source, "Comparisons [{}] between scalars must use the BOOL modifier", opText);
            }
        }
        // set operations are not allowed on scalars
        if (leftIsScalar || rightIsScalar) {
            switch (opType) {
                case AND:
                case UNLESS:
                case OR:
                    throw new ParsingException(source, "Set operator [{}] not allowed in binary scalar expression", opText);
            }
        }

        VectorMatch modifier = VectorMatch.NONE;

        ModifierContext modifierCtx = ctx.modifier();
        if (modifierCtx != null) {
            // modifiers work only on vectors
            if (PromqlDataTypes.isInstantVector(le.dataType()) == false || PromqlDataTypes.isInstantVector(re.dataType()) == false) {
                throw new ParsingException(source, "Vector matching allowed only between instant vectors");
            }

            VectorMatch.Filter filter = modifierCtx.ON() != null ? VectorMatch.Filter.ON : VectorMatch.Filter.IGNORING;
            List<String> filterList = visitLabelList(modifierCtx.modifierLabels);
            VectorMatch.Grouping grouping = VectorMatch.Grouping.NONE;
            List<String> groupingList = visitLabelList(modifierCtx.groupLabels);
            if (modifierCtx.group != null) {
                grouping = modifierCtx.GROUP_LEFT() != null ? VectorMatch.Grouping.LEFT : VectorMatch.Grouping.RIGHT;

                // grouping not allowed with logic operators
                switch (opType) {
                    case AND:
                    case UNLESS:
                    case OR:
                        throw new ParsingException(source(modifierCtx), "No grouping [{}] allowed for [{}] operator", grouping, opText);
                }

                // label declared in ON cannot appear in grouping
                if (modifierCtx.ON() != null) {
                    List<String> repeatedLabels = new ArrayList<>(groupingList);
                    if (filterList.isEmpty() == false && repeatedLabels.retainAll(filterList) && repeatedLabels.isEmpty() == false) {
                        throw new ParsingException(
                            source(modifierCtx.ON()),
                            "Label{} {} must not occur in ON and GROUP clause at once",
                            repeatedLabels.size() > 1 ? "s" : "",
                            repeatedLabels
                        );
                    }

                }
            }

            modifier = new VectorMatch(filter, new LinkedHashSet<>(filterList), grouping, new LinkedHashSet<>(groupingList));
        }

        VectorBinaryOperator.BinaryOp binaryOperator = switch (opType) {
            // arithmetic
            case CARET -> ArithmeticOp.POW;
            case ASTERISK -> ArithmeticOp.MUL;
            case PERCENT -> ArithmeticOp.MOD;
            case SLASH -> ArithmeticOp.DIV;
            case MINUS -> ArithmeticOp.SUB;
            case PLUS -> ArithmeticOp.ADD;
            // comparison
            case EQ -> ComparisonOp.EQ;
            case NEQ -> ComparisonOp.NEQ;
            case LT -> ComparisonOp.LT;
            case LTE -> ComparisonOp.LTE;
            case GT -> ComparisonOp.GT;
            case GTE -> ComparisonOp.GTE;
            // set
            case AND -> SetOp.INTERSECT;
            case UNLESS -> SetOp.SUBTRACT;
            case OR -> SetOp.UNION;
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };

        return switch (binaryOperator) {
            case ArithmeticOp arithmeticOp -> new VectorBinaryArithmetic(source, le, re, modifier, arithmeticOp);
            case ComparisonOp comparisonOp -> new VectorBinaryComparison(source, le, re, modifier, bool, comparisonOp);
            case SetOp setOp -> new VectorBinarySet(source, le, re, modifier, setOp);
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };
    }

    @Override
    public Expression visitParenthesized(ParenthesizedContext ctx) {
        return expression(ctx.expression());
    }

    @Override
    public Subquery visitSubquery(SubqueryContext ctx) {
        Source source = source(ctx);
        Expression expression = expression(ctx.expression());

        if (expression.dataType() != INSTANT_VECTOR) {
            throw new ParsingException(source, "Subquery is only allowed on instant vector, got {}", expression.dataType().typeName());
        }

        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        if (evaluation == null) {
            // TODO: fallback to defaults
        }

        TimeValue range = parseTimeValue(source(ctx.range), ctx.range.getText());
        TimeValue resolution = null;
        TerminalNode idToken = ctx.IDENTIFIER();
        if (idToken != null) {
            String idString = idToken.getText();
            if (idString.startsWith(":") == false) {
                Source resSource = source(ctx.range, idToken.getSymbol());
                throw new ParsingException(resSource, "Invalid subquery range/resolution [{}]", resSource.text());
            }
            resolution = parseTimeValue(source(idToken), idString.substring(1));
        } else {
            // TODO: fallback to defaults
        }
        return new Subquery(source(ctx), expression(ctx.expression()), range, resolution, evaluation);
    }

    @Override
    public Function visitFunction(FunctionContext ctx) {
        Source source = source(ctx);
        String name = ctx.IDENTIFIER().getText().toLowerCase(Locale.ROOT);

        if (PromqlFunctionRegistry.INSTANCE.functionExists(name) == false) {
            throw new ParsingException(source, "unknown function with name [{}]", name);
        }

        List<Expression> arguments = expressions(ctx.expression());
        FunctionResolutionStrategy strategy = FunctionResolutionStrategy.DEFAULT;
        AggregationOperator.Grouping grouping = AggregationOperator.Grouping.NONE;
        if (ctx.functionModifier() != null) {
            FunctionModifierContext modifierContext = ctx.functionModifier();
            grouping = modifierContext.BY() != null ? Grouping.BY : Grouping.WITHOUT;
            List<String> labels = visitLabelList(modifierContext.labelList());
            strategy = new ModifierFunctionResolution(grouping, new LinkedHashSet<>(labels));
        }

        FunctionDefinition def = PromqlFunctionRegistry.INSTANCE.resolveFunction(name);
        // do function validation

        // need exactly 2 params
        if (ParameterizedAggregationOperator.class.isAssignableFrom(def.clazz()) && arguments.size() != 2) {
            throw new ParsingException(
                source,
                "Wrong number of arguments for aggregate expression provided, expected 2, got {}",
                arguments.size()
            );
        }

        UnresolvedFunction unresolved = new UnresolvedFunction(source, name, strategy, arguments);
        Function function = unresolved.buildResolved(null, def);
        // PromQl expects early validation of the tree so let's do it here
        TypeResolution resolution = function.typeResolved();
        if (resolution.unresolved()) {
            throw new ParsingException(source, resolution.message());
        }
        return function;
    }

    @Override
    public Selector visitSelector(SelectorContext ctx) {
        Source source = source(ctx);
        SeriesMatcherContext seriesMatcher = ctx.seriesMatcher();
        String id = visitIdentifier(seriesMatcher.identifier());
        List<LabelMatcher> labels = new ArrayList<>();

        if (id != null) {
            labels.add(new LabelMatcher(NAME, id, Matcher.EQ));
        }
        LabelsContext labelsCtx = seriesMatcher.labels();
        if (labelsCtx != null) {
            // if no name is specified, check for non-empty matchers
            boolean nonEmptyMatcher = id != null;
            for (LabelContext labelCtx : labelsCtx.label()) {
                String kind = labelCtx.kind.getText();
                Matcher matcher = Matcher.from(kind);
                if (matcher == null) {
                    throw new ParsingException(source(labelCtx), "Unrecognized label matcher [{}]", kind);
                }
                String labelName = visitIdentifier(labelCtx.identifier());
                String labelValue = string(labelCtx.STRING());
                if (labelName.contains(":")) {
                    throw new ParsingException(source(labelCtx.identifier()), "[:] not allowed in label names [{}]", labelName);
                }
                // name cannot be defined twice
                if (id != null && NAME.equals(labelName)) {
                    throw new ParsingException(
                        source(labelCtx.identifier()),
                        "Metric name must not be defined twice: [{}] or [{}]",
                        id,
                        labelValue
                    );
                }
                LabelMatcher label = new LabelMatcher(labelName, labelValue, matcher);
                // require at least one empty non-empty matcher
                if (nonEmptyMatcher == false && label.matchesEmpty() == false) {
                    nonEmptyMatcher = true;
                }
                labels.add(label);
            }
            if (nonEmptyMatcher == false) {
                throw new ParsingException(source(labelsCtx), "Vector selector must contain at least one non-empty matcher");
            }
        }
        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        TimeValue range = visitDuration(ctx.duration());
        // fall back to default
        if (evaluation == null) {
            evaluation = new Evaluation(start);
        }
        return range == null ? new InstantSelector(source, labels, evaluation) : new RangeSelector(source, labels, range, evaluation);
    }

    @Override
    public List<String> visitLabelList(LabelListContext ctx) {
        return ctx != null ? visitList(this, ctx.identifier(), String.class) : emptyList();
    }

    @Override
    public Evaluation visitEvaluation(EvaluationContext ctx) {
        if (ctx == null) {
            return null;
        }

        TimeValue offset = null;
        boolean negativeOffset = false;
        Instant at = null;

        AtContext atCtx = ctx.at();
        if (atCtx != null) {
            Source source = source(atCtx);
            if (atCtx.AT_START() != null) {
                at = start;
            } else if (atCtx.AT_END() != null) {
                at = stop;
            } else {
                Object value = visit(atCtx.number());
                if (value instanceof Literal == false || ((Literal) value).fold() instanceof Number == false) {
                    throw new ParsingException(source, "Expected number but got {}", value);
                }
                Number number = (Number) ((Literal) value).fold();
                // the value can have a floating point
                double millis = number.doubleValue() * 1000;

                if (Double.isInfinite(millis)) {
                    throw new ParsingException(source, "Value [{}] is too large", source.text());
                }
                if (Double.isNaN(millis)) {
                    throw new ParsingException(source, "[{}] cannot be parsed as a number (NaN)", millis);
                }
                // force casting - loss is acceptable in Promql
                long millisLong = (long) millis;

                if (atCtx.MINUS() != null) {
                    if (millisLong == Long.MIN_VALUE) {
                        throw new ParsingException(source, "Value [{}] cannot be negated due to underflow", millisLong);
                    }
                    millisLong = -millisLong;
                }
                at = Instant.ofEpochMilli(millisLong);
            }
        }
        OffsetContext offsetContext = ctx.offset();
        if (offsetContext != null) {
            offset = visitDuration(offsetContext.duration());
            negativeOffset = offsetContext.MINUS() != null;
        }
        return new Evaluation(offset, negativeOffset, at);
    }

    @Override
    public TimeValue visitDuration(DurationContext ctx) {
        if (ctx == null) {
            return null;
        }

        return parseTimeValue(source(ctx), text(ctx.TIME_VALUE()));
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            double value;
            String s = text.toLowerCase(Locale.ROOT);
            if ("inf".equals(s)) {
                value = Double.POSITIVE_INFINITY;
            } else if ("nan".equals(s)) {
                value = Double.NaN;
            } else {
                value = Double.parseDouble(text);
            }
            return new Scalar(source, value);
        } catch (NumberFormatException ne) {
            throw new ParsingException(source, "Cannot parse number [{}]", text);
        }
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        long value;

        try {
            value = StringUtils.parseLong(text);
        } catch (QlIllegalArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                // use DataTypes.DOUBLE for precise type
                return new Scalar(source, StringUtils.parseDouble(text));
            } catch (QlIllegalArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }

        Number val = value;

        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            val = (int) value;
        }
        // use type instead for precise type
        return new Scalar(source, val.doubleValue());
    }

    @Override
    public Literal visitHexLiteral(HexLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        DataType type = DataTypes.LONG;
        Object val;

        // remove leading 0x
        long value;
        try {
            value = Long.parseLong(text.substring(2), 16);
        } catch (NumberFormatException e) {
            throw new ParsingException(source, "Cannot parse hexadecimal expression [{}]", text);
        }

        // try to downsize to int
        if ((int) value == value) {
            type = DataTypes.INTEGER;
            val = (int) value;
        } else {
            val = value;
        }
        // use type for precise dataType
        return new Scalar(source, (double) val);
    }

    @Override
    public Literal visitString(StringContext ctx) {
        Source source = source(ctx);
        // previously DataTypes.KEYWORD
        return new Literal(source, string(ctx.STRING()), STRING);
    }

    private static TimeValue parseTimeValue(Source source, String text) {
        TimeValue timeValue = ParsingUtils.parseTimeValue(source, text);
        if (timeValue.duration() == 0) {
            throw new ParsingException(source, "Invalid time duration [{}], zero value specified", text);
        }
        return timeValue;
    }
}
