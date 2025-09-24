// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class PromqlBaseParser extends ParserConfig {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    PLUS=1, MINUS=2, ASTERISK=3, SLASH=4, PERCENT=5, CARET=6, EQ=7, NEQ=8, 
    GT=9, GTE=10, LT=11, LTE=12, LABEL_EQ=13, LABEL_RGX=14, LABEL_RGX_NEQ=15, 
    AND=16, OR=17, UNLESS=18, BY=19, WITHOUT=20, ON=21, IGNORING=22, GROUP_LEFT=23, 
    GROUP_RIGHT=24, BOOL=25, OFFSET=26, AT=27, AT_START=28, AT_END=29, LCB=30, 
    RCB=31, LSB=32, RSB=33, LP=34, RP=35, COLON=36, COMMA=37, STRING=38, INTEGER_VALUE=39, 
    DECIMAL_VALUE=40, HEXADECIMAL=41, TIME_VALUE_WITH_COLON=42, TIME_VALUE=43, 
    IDENTIFIER=44, COMMENT=45, WS=46, UNRECOGNIZED=47;
  public static final int
    RULE_singleExpression = 0, RULE_expression = 1, RULE_subqueryResolution = 2, 
    RULE_value = 3, RULE_function = 4, RULE_functionModifier = 5, RULE_selector = 6, 
    RULE_seriesMatcher = 7, RULE_modifier = 8, RULE_labelList = 9, RULE_labels = 10, 
    RULE_label = 11, RULE_labelName = 12, RULE_identifier = 13, RULE_evaluation = 14, 
    RULE_offset = 15, RULE_duration = 16, RULE_at = 17, RULE_constant = 18, 
    RULE_number = 19, RULE_string = 20, RULE_timeValue = 21, RULE_nonReserved = 22;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleExpression", "expression", "subqueryResolution", "value", "function", 
      "functionModifier", "selector", "seriesMatcher", "modifier", "labelList", 
      "labels", "label", "labelName", "identifier", "evaluation", "offset", 
      "duration", "at", "constant", "number", "string", "timeValue", "nonReserved"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, "'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'=='", "'!='", "'>'", 
      "'>='", "'<'", "'<='", "'='", "'=~'", "'!~'", "'and'", "'or'", "'unless'", 
      "'by'", "'without'", "'on'", "'ignoring'", "'group_left'", "'group_right'", 
      "'bool'", null, "'@'", "'start()'", "'end()'", "'{'", "'}'", "'['", "']'", 
      "'('", "')'", "':'", "','"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CARET", "EQ", 
      "NEQ", "GT", "GTE", "LT", "LTE", "LABEL_EQ", "LABEL_RGX", "LABEL_RGX_NEQ", 
      "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", "IGNORING", "GROUP_LEFT", 
      "GROUP_RIGHT", "BOOL", "OFFSET", "AT", "AT_START", "AT_END", "LCB", "RCB", 
      "LSB", "RSB", "LP", "RP", "COLON", "COMMA", "STRING", "INTEGER_VALUE", 
      "DECIMAL_VALUE", "HEXADECIMAL", "TIME_VALUE_WITH_COLON", "TIME_VALUE", 
      "IDENTIFIER", "COMMENT", "WS", "UNRECOGNIZED"
    };
  }
  private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() { return "PromqlBaseParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  @SuppressWarnings("this-escape")
  public PromqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SingleExpressionContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode EOF() { return getToken(PromqlBaseParser.EOF, 0); }
    @SuppressWarnings("this-escape")
    public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSingleExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSingleExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSingleExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleExpressionContext singleExpression() throws RecognitionException {
    SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(46);
      expression(0);
      setState(47);
      match(EOF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_expression; }
   
    @SuppressWarnings("this-escape")
    public ExpressionContext() { }
    public void copyFrom(ExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionContext extends ExpressionContext {
    public ValueContext value() {
      return getRuleContext(ValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterValueExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitValueExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitValueExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryContext extends ExpressionContext {
    public DurationContext range;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode LSB() { return getToken(PromqlBaseParser.LSB, 0); }
    public SubqueryResolutionContext subqueryResolution() {
      return getRuleContext(SubqueryResolutionContext.class,0);
    }
    public TerminalNode RSB() { return getToken(PromqlBaseParser.RSB, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public EvaluationContext evaluation() {
      return getRuleContext(EvaluationContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SubqueryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSubquery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSubquery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ParenthesizedContext extends ExpressionContext {
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ParenthesizedContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterParenthesized(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitParenthesized(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitParenthesized(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticBinaryContext extends ExpressionContext {
    public ExpressionContext left;
    public Token op;
    public ExpressionContext right;
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public TerminalNode CARET() { return getToken(PromqlBaseParser.CARET, 0); }
    public ModifierContext modifier() {
      return getRuleContext(ModifierContext.class,0);
    }
    public TerminalNode ASTERISK() { return getToken(PromqlBaseParser.ASTERISK, 0); }
    public TerminalNode PERCENT() { return getToken(PromqlBaseParser.PERCENT, 0); }
    public TerminalNode SLASH() { return getToken(PromqlBaseParser.SLASH, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    public TerminalNode EQ() { return getToken(PromqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(PromqlBaseParser.NEQ, 0); }
    public TerminalNode GT() { return getToken(PromqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(PromqlBaseParser.GTE, 0); }
    public TerminalNode LT() { return getToken(PromqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(PromqlBaseParser.LTE, 0); }
    public TerminalNode BOOL() { return getToken(PromqlBaseParser.BOOL, 0); }
    public TerminalNode AND() { return getToken(PromqlBaseParser.AND, 0); }
    public TerminalNode UNLESS() { return getToken(PromqlBaseParser.UNLESS, 0); }
    public TerminalNode OR() { return getToken(PromqlBaseParser.OR, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticBinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticUnaryContext extends ExpressionContext {
    public Token operator;
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticUnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    return expression(0);
  }

  private ExpressionContext expression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
    ExpressionContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_expression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(57);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PLUS:
      case MINUS:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(50);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(51);
        expression(9);
        }
        break;
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case LCB:
      case STRING:
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
      case TIME_VALUE_WITH_COLON:
      case TIME_VALUE:
      case IDENTIFIER:
        {
        _localctx = new ValueExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(52);
        value();
        }
        break;
      case LP:
        {
        _localctx = new ParenthesizedContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(53);
        match(LP);
        setState(54);
        expression(0);
        setState(55);
        match(RP);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
      _ctx.stop = _input.LT(-1);
      setState(108);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(106);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(59);
            if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
            setState(60);
            ((ArithmeticBinaryContext)_localctx).op = match(CARET);
            setState(62);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
            case 1:
              {
              setState(61);
              modifier();
              }
              break;
            }
            setState(64);
            ((ArithmeticBinaryContext)_localctx).right = expression(10);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(65);
            if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
            setState(66);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 56L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(68);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
            case 1:
              {
              setState(67);
              modifier();
              }
              break;
            }
            setState(70);
            ((ArithmeticBinaryContext)_localctx).right = expression(9);
            }
            break;
          case 3:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(71);
            if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
            setState(72);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(74);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
            case 1:
              {
              setState(73);
              modifier();
              }
              break;
            }
            setState(76);
            ((ArithmeticBinaryContext)_localctx).right = expression(8);
            }
            break;
          case 4:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(77);
            if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
            setState(78);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 8064L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(80);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
            case 1:
              {
              setState(79);
              match(BOOL);
              }
              break;
            }
            setState(83);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
            case 1:
              {
              setState(82);
              modifier();
              }
              break;
            }
            setState(85);
            ((ArithmeticBinaryContext)_localctx).right = expression(7);
            }
            break;
          case 5:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(86);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(87);
            ((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==AND || _la==UNLESS) ) {
              ((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(89);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
            case 1:
              {
              setState(88);
              modifier();
              }
              break;
            }
            setState(91);
            ((ArithmeticBinaryContext)_localctx).right = expression(6);
            }
            break;
          case 6:
            {
            _localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(92);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(93);
            ((ArithmeticBinaryContext)_localctx).op = match(OR);
            setState(95);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
            case 1:
              {
              setState(94);
              modifier();
              }
              break;
            }
            setState(97);
            ((ArithmeticBinaryContext)_localctx).right = expression(5);
            }
            break;
          case 7:
            {
            _localctx = new SubqueryContext(new ExpressionContext(_parentctx, _parentState));
            pushNewRecursionContext(_localctx, _startState, RULE_expression);
            setState(98);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(99);
            match(LSB);
            setState(100);
            ((SubqueryContext)_localctx).range = duration();
            setState(101);
            subqueryResolution();
            setState(102);
            match(RSB);
            setState(104);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
            case 1:
              {
              setState(103);
              evaluation();
              }
              break;
            }
            }
            break;
          }
          } 
        }
        setState(110);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryResolutionContext extends ParserRuleContext {
    public DurationContext resolution;
    public Token op;
    public TerminalNode COLON() { return getToken(PromqlBaseParser.COLON, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode TIME_VALUE_WITH_COLON() { return getToken(PromqlBaseParser.TIME_VALUE_WITH_COLON, 0); }
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    public TerminalNode CARET() { return getToken(PromqlBaseParser.CARET, 0); }
    public TerminalNode ASTERISK() { return getToken(PromqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(PromqlBaseParser.SLASH, 0); }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(PromqlBaseParser.PLUS, 0); }
    @SuppressWarnings("this-escape")
    public SubqueryResolutionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subqueryResolution; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSubqueryResolution(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSubqueryResolution(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSubqueryResolution(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryResolutionContext subqueryResolution() throws RecognitionException {
    SubqueryResolutionContext _localctx = new SubqueryResolutionContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_subqueryResolution);
    int _la;
    try {
      setState(125);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(111);
        match(COLON);
        setState(113);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 34927881945094L) != 0)) {
          {
          setState(112);
          ((SubqueryResolutionContext)_localctx).resolution = duration();
          }
        }

        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(115);
        match(TIME_VALUE_WITH_COLON);
        setState(116);
        ((SubqueryResolutionContext)_localctx).op = match(CARET);
        setState(117);
        expression(0);
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(118);
        match(TIME_VALUE_WITH_COLON);
        setState(119);
        ((SubqueryResolutionContext)_localctx).op = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASTERISK || _la==SLASH) ) {
          ((SubqueryResolutionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(120);
        expression(0);
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(121);
        match(TIME_VALUE_WITH_COLON);
        setState(122);
        ((SubqueryResolutionContext)_localctx).op = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((SubqueryResolutionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(123);
        expression(0);
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(124);
        match(TIME_VALUE_WITH_COLON);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ValueContext extends ParserRuleContext {
    public FunctionContext function() {
      return getRuleContext(FunctionContext.class,0);
    }
    public SelectorContext selector() {
      return getRuleContext(SelectorContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_value; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueContext value() throws RecognitionException {
    ValueContext _localctx = new ValueContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_value);
    try {
      setState(130);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(127);
        function();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(128);
        selector();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(129);
        constant();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionContext extends ParserRuleContext {
    public TerminalNode IDENTIFIER() { return getToken(PromqlBaseParser.IDENTIFIER, 0); }
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }
    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    public FunctionModifierContext functionModifier() {
      return getRuleContext(FunctionModifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_function; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionContext function() throws RecognitionException {
    FunctionContext _localctx = new FunctionContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_function);
    int _la;
    try {
      setState(162);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(132);
        match(IDENTIFIER);
        setState(133);
        match(LP);
        setState(134);
        match(RP);
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(135);
        match(IDENTIFIER);
        setState(136);
        match(LP);
        setState(137);
        expression(0);
        setState(142);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(138);
          match(COMMA);
          setState(139);
          expression(0);
          }
          }
          setState(144);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(145);
        match(RP);
        setState(147);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
        case 1:
          {
          setState(146);
          functionModifier();
          }
          break;
        }
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(149);
        match(IDENTIFIER);
        setState(150);
        functionModifier();
        setState(151);
        match(LP);
        setState(152);
        expression(0);
        setState(157);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(153);
          match(COMMA);
          setState(154);
          expression(0);
          }
          }
          setState(159);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(160);
        match(RP);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionModifierContext extends ParserRuleContext {
    public LabelListContext labelList() {
      return getRuleContext(LabelListContext.class,0);
    }
    public TerminalNode BY() { return getToken(PromqlBaseParser.BY, 0); }
    public TerminalNode WITHOUT() { return getToken(PromqlBaseParser.WITHOUT, 0); }
    @SuppressWarnings("this-escape")
    public FunctionModifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionModifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterFunctionModifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitFunctionModifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitFunctionModifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionModifierContext functionModifier() throws RecognitionException {
    FunctionModifierContext _localctx = new FunctionModifierContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_functionModifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(164);
      _la = _input.LA(1);
      if ( !(_la==BY || _la==WITHOUT) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(165);
      labelList();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SelectorContext extends ParserRuleContext {
    public SeriesMatcherContext seriesMatcher() {
      return getRuleContext(SeriesMatcherContext.class,0);
    }
    public TerminalNode LSB() { return getToken(PromqlBaseParser.LSB, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode RSB() { return getToken(PromqlBaseParser.RSB, 0); }
    public EvaluationContext evaluation() {
      return getRuleContext(EvaluationContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SelectorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selector; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSelector(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSelector(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSelector(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectorContext selector() throws RecognitionException {
    SelectorContext _localctx = new SelectorContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_selector);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(167);
      seriesMatcher();
      setState(172);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
      case 1:
        {
        setState(168);
        match(LSB);
        setState(169);
        duration();
        setState(170);
        match(RSB);
        }
        break;
      }
      setState(175);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
      case 1:
        {
        setState(174);
        evaluation();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SeriesMatcherContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode LCB() { return getToken(PromqlBaseParser.LCB, 0); }
    public TerminalNode RCB() { return getToken(PromqlBaseParser.RCB, 0); }
    public LabelsContext labels() {
      return getRuleContext(LabelsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SeriesMatcherContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_seriesMatcher; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterSeriesMatcher(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitSeriesMatcher(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitSeriesMatcher(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SeriesMatcherContext seriesMatcher() throws RecognitionException {
    SeriesMatcherContext _localctx = new SeriesMatcherContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_seriesMatcher);
    int _la;
    try {
      setState(189);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(177);
        identifier();
        setState(183);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
        case 1:
          {
          setState(178);
          match(LCB);
          setState(180);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
            {
            setState(179);
            labels();
            }
          }

          setState(182);
          match(RCB);
          }
          break;
        }
        }
        break;
      case LCB:
        enterOuterAlt(_localctx, 2);
        {
        setState(185);
        match(LCB);
        setState(186);
        labels();
        setState(187);
        match(RCB);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ModifierContext extends ParserRuleContext {
    public LabelListContext modifierLabels;
    public Token group;
    public LabelListContext groupLabels;
    public TerminalNode IGNORING() { return getToken(PromqlBaseParser.IGNORING, 0); }
    public TerminalNode ON() { return getToken(PromqlBaseParser.ON, 0); }
    public List<LabelListContext> labelList() {
      return getRuleContexts(LabelListContext.class);
    }
    public LabelListContext labelList(int i) {
      return getRuleContext(LabelListContext.class,i);
    }
    public TerminalNode GROUP_LEFT() { return getToken(PromqlBaseParser.GROUP_LEFT, 0); }
    public TerminalNode GROUP_RIGHT() { return getToken(PromqlBaseParser.GROUP_RIGHT, 0); }
    @SuppressWarnings("this-escape")
    public ModifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_modifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterModifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitModifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitModifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ModifierContext modifier() throws RecognitionException {
    ModifierContext _localctx = new ModifierContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_modifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(191);
      _la = _input.LA(1);
      if ( !(_la==ON || _la==IGNORING) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(192);
      ((ModifierContext)_localctx).modifierLabels = labelList();
      setState(197);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        {
        setState(193);
        ((ModifierContext)_localctx).group = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==GROUP_LEFT || _la==GROUP_RIGHT) ) {
          ((ModifierContext)_localctx).group = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(195);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
        case 1:
          {
          setState(194);
          ((ModifierContext)_localctx).groupLabels = labelList();
          }
          break;
        }
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LabelListContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(PromqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(PromqlBaseParser.RP, 0); }
    public List<LabelNameContext> labelName() {
      return getRuleContexts(LabelNameContext.class);
    }
    public LabelNameContext labelName(int i) {
      return getRuleContext(LabelNameContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LabelListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labelList; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabelList(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabelList(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabelList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelListContext labelList() throws RecognitionException {
    LabelListContext _localctx = new LabelListContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_labelList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(199);
      match(LP);
      setState(206);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
        {
        {
        setState(200);
        labelName();
        setState(202);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(201);
          match(COMMA);
          }
        }

        }
        }
        setState(208);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(209);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LabelsContext extends ParserRuleContext {
    public List<LabelContext> label() {
      return getRuleContexts(LabelContext.class);
    }
    public LabelContext label(int i) {
      return getRuleContext(LabelContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(PromqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(PromqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LabelsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labels; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabels(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabels(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabels(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelsContext labels() throws RecognitionException {
    LabelsContext _localctx = new LabelsContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_labels);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(211);
      label();
      setState(218);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(212);
        match(COMMA);
        setState(214);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 21715488800768L) != 0)) {
          {
          setState(213);
          label();
          }
        }

        }
        }
        setState(220);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LabelContext extends ParserRuleContext {
    public Token kind;
    public LabelNameContext labelName() {
      return getRuleContext(LabelNameContext.class,0);
    }
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    public TerminalNode LABEL_EQ() { return getToken(PromqlBaseParser.LABEL_EQ, 0); }
    public TerminalNode NEQ() { return getToken(PromqlBaseParser.NEQ, 0); }
    public TerminalNode LABEL_RGX() { return getToken(PromqlBaseParser.LABEL_RGX, 0); }
    public TerminalNode LABEL_RGX_NEQ() { return getToken(PromqlBaseParser.LABEL_RGX_NEQ, 0); }
    @SuppressWarnings("this-escape")
    public LabelContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_label; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabel(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabel(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabel(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelContext label() throws RecognitionException {
    LabelContext _localctx = new LabelContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_label);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(221);
      labelName();
      setState(224);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 57600L) != 0)) {
        {
        setState(222);
        ((LabelContext)_localctx).kind = _input.LT(1);
        _la = _input.LA(1);
        if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 57600L) != 0)) ) {
          ((LabelContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(223);
        match(STRING);
        }
      }

      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LabelNameContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LabelNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_labelName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterLabelName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitLabelName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitLabelName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LabelNameContext labelName() throws RecognitionException {
    LabelNameContext _localctx = new LabelNameContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_labelName);
    try {
      setState(229);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(226);
        identifier();
        }
        break;
      case STRING:
        enterOuterAlt(_localctx, 2);
        {
        setState(227);
        match(STRING);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(228);
        number();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode IDENTIFIER() { return getToken(PromqlBaseParser.IDENTIFIER, 0); }
    public NonReservedContext nonReserved() {
      return getRuleContext(NonReservedContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_identifier);
    try {
      setState(233);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(231);
        match(IDENTIFIER);
        }
        break;
      case AND:
      case OR:
      case UNLESS:
      case BY:
      case WITHOUT:
      case ON:
      case IGNORING:
      case GROUP_LEFT:
      case GROUP_RIGHT:
      case BOOL:
      case OFFSET:
        enterOuterAlt(_localctx, 2);
        {
        setState(232);
        nonReserved();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EvaluationContext extends ParserRuleContext {
    public OffsetContext offset() {
      return getRuleContext(OffsetContext.class,0);
    }
    public AtContext at() {
      return getRuleContext(AtContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EvaluationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_evaluation; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterEvaluation(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitEvaluation(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitEvaluation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EvaluationContext evaluation() throws RecognitionException {
    EvaluationContext _localctx = new EvaluationContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_evaluation);
    try {
      setState(243);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case OFFSET:
        enterOuterAlt(_localctx, 1);
        {
        setState(235);
        offset();
        setState(237);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
        case 1:
          {
          setState(236);
          at();
          }
          break;
        }
        }
        break;
      case AT:
        enterOuterAlt(_localctx, 2);
        {
        setState(239);
        at();
        setState(241);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
        case 1:
          {
          setState(240);
          offset();
          }
          break;
        }
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class OffsetContext extends ParserRuleContext {
    public TerminalNode OFFSET() { return getToken(PromqlBaseParser.OFFSET, 0); }
    public DurationContext duration() {
      return getRuleContext(DurationContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public OffsetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_offset; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterOffset(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitOffset(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitOffset(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OffsetContext offset() throws RecognitionException {
    OffsetContext _localctx = new OffsetContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_offset);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(245);
      match(OFFSET);
      setState(247);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
      case 1:
        {
        setState(246);
        match(MINUS);
        }
        break;
      }
      setState(249);
      duration();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DurationContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DurationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_duration; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterDuration(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitDuration(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitDuration(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DurationContext duration() throws RecognitionException {
    DurationContext _localctx = new DurationContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_duration);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(251);
      expression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class AtContext extends ParserRuleContext {
    public TerminalNode AT() { return getToken(PromqlBaseParser.AT, 0); }
    public TimeValueContext timeValue() {
      return getRuleContext(TimeValueContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(PromqlBaseParser.MINUS, 0); }
    public TerminalNode AT_START() { return getToken(PromqlBaseParser.AT_START, 0); }
    public TerminalNode AT_END() { return getToken(PromqlBaseParser.AT_END, 0); }
    @SuppressWarnings("this-escape")
    public AtContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_at; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterAt(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitAt(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitAt(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AtContext at() throws RecognitionException {
    AtContext _localctx = new AtContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_at);
    int _la;
    try {
      setState(260);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,37,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(253);
        match(AT);
        setState(255);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==MINUS) {
          {
          setState(254);
          match(MINUS);
          }
        }

        setState(257);
        timeValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(258);
        match(AT);
        setState(259);
        _la = _input.LA(1);
        if ( !(_la==AT_START || _la==AT_END) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ConstantContext extends ParserRuleContext {
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TimeValueContext timeValue() {
      return getRuleContext(TimeValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterConstant(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitConstant(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitConstant(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_constant);
    try {
      setState(265);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(262);
        number();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(263);
        string();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(264);
        timeValue();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class NumberContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public NumberContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_number; }
   
    @SuppressWarnings("this-escape")
    public NumberContext() { }
    public void copyFrom(NumberContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends NumberContext {
    public TerminalNode DECIMAL_VALUE() { return getToken(PromqlBaseParser.DECIMAL_VALUE, 0); }
    @SuppressWarnings("this-escape")
    public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends NumberContext {
    public TerminalNode INTEGER_VALUE() { return getToken(PromqlBaseParser.INTEGER_VALUE, 0); }
    @SuppressWarnings("this-escape")
    public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class HexLiteralContext extends NumberContext {
    public TerminalNode HEXADECIMAL() { return getToken(PromqlBaseParser.HEXADECIMAL, 0); }
    @SuppressWarnings("this-escape")
    public HexLiteralContext(NumberContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterHexLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitHexLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitHexLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_number);
    try {
      setState(270);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DECIMAL_VALUE:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(267);
        match(DECIMAL_VALUE);
        }
        break;
      case INTEGER_VALUE:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(268);
        match(INTEGER_VALUE);
        }
        break;
      case HEXADECIMAL:
        _localctx = new HexLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(269);
        match(HEXADECIMAL);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class StringContext extends ParserRuleContext {
    public TerminalNode STRING() { return getToken(PromqlBaseParser.STRING, 0); }
    @SuppressWarnings("this-escape")
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(272);
      match(STRING);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class TimeValueContext extends ParserRuleContext {
    public TerminalNode TIME_VALUE_WITH_COLON() { return getToken(PromqlBaseParser.TIME_VALUE_WITH_COLON, 0); }
    public TerminalNode TIME_VALUE() { return getToken(PromqlBaseParser.TIME_VALUE, 0); }
    public NumberContext number() {
      return getRuleContext(NumberContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public TimeValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_timeValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterTimeValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitTimeValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitTimeValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TimeValueContext timeValue() throws RecognitionException {
    TimeValueContext _localctx = new TimeValueContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_timeValue);
    try {
      setState(277);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case TIME_VALUE_WITH_COLON:
        enterOuterAlt(_localctx, 1);
        {
        setState(274);
        match(TIME_VALUE_WITH_COLON);
        }
        break;
      case TIME_VALUE:
        enterOuterAlt(_localctx, 2);
        {
        setState(275);
        match(TIME_VALUE);
        }
        break;
      case INTEGER_VALUE:
      case DECIMAL_VALUE:
      case HEXADECIMAL:
        enterOuterAlt(_localctx, 3);
        {
        setState(276);
        number();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class NonReservedContext extends ParserRuleContext {
    public TerminalNode AND() { return getToken(PromqlBaseParser.AND, 0); }
    public TerminalNode BOOL() { return getToken(PromqlBaseParser.BOOL, 0); }
    public TerminalNode BY() { return getToken(PromqlBaseParser.BY, 0); }
    public TerminalNode GROUP_LEFT() { return getToken(PromqlBaseParser.GROUP_LEFT, 0); }
    public TerminalNode GROUP_RIGHT() { return getToken(PromqlBaseParser.GROUP_RIGHT, 0); }
    public TerminalNode IGNORING() { return getToken(PromqlBaseParser.IGNORING, 0); }
    public TerminalNode OFFSET() { return getToken(PromqlBaseParser.OFFSET, 0); }
    public TerminalNode OR() { return getToken(PromqlBaseParser.OR, 0); }
    public TerminalNode ON() { return getToken(PromqlBaseParser.ON, 0); }
    public TerminalNode UNLESS() { return getToken(PromqlBaseParser.UNLESS, 0); }
    public TerminalNode WITHOUT() { return getToken(PromqlBaseParser.WITHOUT, 0); }
    @SuppressWarnings("this-escape")
    public NonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_nonReserved; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).enterNonReserved(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof PromqlBaseParserListener ) ((PromqlBaseParserListener)listener).exitNonReserved(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof PromqlBaseParserVisitor ) return ((PromqlBaseParserVisitor<? extends T>)visitor).visitNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NonReservedContext nonReserved() throws RecognitionException {
    NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(279);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 134152192L) != 0)) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return expression_sempred((ExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 10);
    case 1:
      return precpred(_ctx, 8);
    case 2:
      return precpred(_ctx, 7);
    case 3:
      return precpred(_ctx, 6);
    case 4:
      return precpred(_ctx, 5);
    case 5:
      return precpred(_ctx, 4);
    case 6:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001/\u011a\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
    "\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
    "\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
    "\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b\u0002"+
    "\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007\u000f"+
    "\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007\u0012"+
    "\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007\u0015"+
    "\u0002\u0016\u0007\u0016\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0003\u0001:\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0003\u0001?\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0003\u0001E\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0003\u0001K\b\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
    "\u0003\u0001Q\b\u0001\u0001\u0001\u0003\u0001T\b\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0003\u0001Z\b\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0003\u0001`\b\u0001\u0001\u0001\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003"+
    "\u0001i\b\u0001\u0005\u0001k\b\u0001\n\u0001\f\u0001n\t\u0001\u0001\u0002"+
    "\u0001\u0002\u0003\u0002r\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0003\u0002~\b\u0002\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0003\u0003\u0083\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0005\u0004\u008d\b\u0004"+
    "\n\u0004\f\u0004\u0090\t\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u0094"+
    "\b\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001"+
    "\u0004\u0005\u0004\u009c\b\u0004\n\u0004\f\u0004\u009f\t\u0004\u0001\u0004"+
    "\u0001\u0004\u0003\u0004\u00a3\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005"+
    "\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0003\u0006"+
    "\u00ad\b\u0006\u0001\u0006\u0003\u0006\u00b0\b\u0006\u0001\u0007\u0001"+
    "\u0007\u0001\u0007\u0003\u0007\u00b5\b\u0007\u0001\u0007\u0003\u0007\u00b8"+
    "\b\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0003\u0007\u00be"+
    "\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0003\b\u00c4\b\b\u0003\b\u00c6"+
    "\b\b\u0001\t\u0001\t\u0001\t\u0003\t\u00cb\b\t\u0005\t\u00cd\b\t\n\t\f"+
    "\t\u00d0\t\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0003\n\u00d7\b\n"+
    "\u0005\n\u00d9\b\n\n\n\f\n\u00dc\t\n\u0001\u000b\u0001\u000b\u0001\u000b"+
    "\u0003\u000b\u00e1\b\u000b\u0001\f\u0001\f\u0001\f\u0003\f\u00e6\b\f\u0001"+
    "\r\u0001\r\u0003\r\u00ea\b\r\u0001\u000e\u0001\u000e\u0003\u000e\u00ee"+
    "\b\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u00f2\b\u000e\u0003\u000e"+
    "\u00f4\b\u000e\u0001\u000f\u0001\u000f\u0003\u000f\u00f8\b\u000f\u0001"+
    "\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0003"+
    "\u0011\u0100\b\u0011\u0001\u0011\u0001\u0011\u0001\u0011\u0003\u0011\u0105"+
    "\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012\u010a\b\u0012"+
    "\u0001\u0013\u0001\u0013\u0001\u0013\u0003\u0013\u010f\b\u0013\u0001\u0014"+
    "\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0003\u0015\u0116\b\u0015"+
    "\u0001\u0016\u0001\u0016\u0001\u0016\u0000\u0001\u0002\u0017\u0000\u0002"+
    "\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e"+
    " \"$&(*,\u0000\u000b\u0001\u0000\u0001\u0002\u0001\u0000\u0003\u0005\u0001"+
    "\u0000\u0007\f\u0002\u0000\u0010\u0010\u0012\u0012\u0001\u0000\u0003\u0004"+
    "\u0001\u0000\u0013\u0014\u0001\u0000\u0015\u0016\u0001\u0000\u0017\u0018"+
    "\u0002\u0000\b\b\r\u000f\u0001\u0000\u001c\u001d\u0001\u0000\u0010\u001a"+
    "\u013a\u0000.\u0001\u0000\u0000\u0000\u00029\u0001\u0000\u0000\u0000\u0004"+
    "}\u0001\u0000\u0000\u0000\u0006\u0082\u0001\u0000\u0000\u0000\b\u00a2"+
    "\u0001\u0000\u0000\u0000\n\u00a4\u0001\u0000\u0000\u0000\f\u00a7\u0001"+
    "\u0000\u0000\u0000\u000e\u00bd\u0001\u0000\u0000\u0000\u0010\u00bf\u0001"+
    "\u0000\u0000\u0000\u0012\u00c7\u0001\u0000\u0000\u0000\u0014\u00d3\u0001"+
    "\u0000\u0000\u0000\u0016\u00dd\u0001\u0000\u0000\u0000\u0018\u00e5\u0001"+
    "\u0000\u0000\u0000\u001a\u00e9\u0001\u0000\u0000\u0000\u001c\u00f3\u0001"+
    "\u0000\u0000\u0000\u001e\u00f5\u0001\u0000\u0000\u0000 \u00fb\u0001\u0000"+
    "\u0000\u0000\"\u0104\u0001\u0000\u0000\u0000$\u0109\u0001\u0000\u0000"+
    "\u0000&\u010e\u0001\u0000\u0000\u0000(\u0110\u0001\u0000\u0000\u0000*"+
    "\u0115\u0001\u0000\u0000\u0000,\u0117\u0001\u0000\u0000\u0000./\u0003"+
    "\u0002\u0001\u0000/0\u0005\u0000\u0000\u00010\u0001\u0001\u0000\u0000"+
    "\u000012\u0006\u0001\uffff\uffff\u000023\u0007\u0000\u0000\u00003:\u0003"+
    "\u0002\u0001\t4:\u0003\u0006\u0003\u000056\u0005\"\u0000\u000067\u0003"+
    "\u0002\u0001\u000078\u0005#\u0000\u00008:\u0001\u0000\u0000\u000091\u0001"+
    "\u0000\u0000\u000094\u0001\u0000\u0000\u000095\u0001\u0000\u0000\u0000"+
    ":l\u0001\u0000\u0000\u0000;<\n\n\u0000\u0000<>\u0005\u0006\u0000\u0000"+
    "=?\u0003\u0010\b\u0000>=\u0001\u0000\u0000\u0000>?\u0001\u0000\u0000\u0000"+
    "?@\u0001\u0000\u0000\u0000@k\u0003\u0002\u0001\nAB\n\b\u0000\u0000BD\u0007"+
    "\u0001\u0000\u0000CE\u0003\u0010\b\u0000DC\u0001\u0000\u0000\u0000DE\u0001"+
    "\u0000\u0000\u0000EF\u0001\u0000\u0000\u0000Fk\u0003\u0002\u0001\tGH\n"+
    "\u0007\u0000\u0000HJ\u0007\u0000\u0000\u0000IK\u0003\u0010\b\u0000JI\u0001"+
    "\u0000\u0000\u0000JK\u0001\u0000\u0000\u0000KL\u0001\u0000\u0000\u0000"+
    "Lk\u0003\u0002\u0001\bMN\n\u0006\u0000\u0000NP\u0007\u0002\u0000\u0000"+
    "OQ\u0005\u0019\u0000\u0000PO\u0001\u0000\u0000\u0000PQ\u0001\u0000\u0000"+
    "\u0000QS\u0001\u0000\u0000\u0000RT\u0003\u0010\b\u0000SR\u0001\u0000\u0000"+
    "\u0000ST\u0001\u0000\u0000\u0000TU\u0001\u0000\u0000\u0000Uk\u0003\u0002"+
    "\u0001\u0007VW\n\u0005\u0000\u0000WY\u0007\u0003\u0000\u0000XZ\u0003\u0010"+
    "\b\u0000YX\u0001\u0000\u0000\u0000YZ\u0001\u0000\u0000\u0000Z[\u0001\u0000"+
    "\u0000\u0000[k\u0003\u0002\u0001\u0006\\]\n\u0004\u0000\u0000]_\u0005"+
    "\u0011\u0000\u0000^`\u0003\u0010\b\u0000_^\u0001\u0000\u0000\u0000_`\u0001"+
    "\u0000\u0000\u0000`a\u0001\u0000\u0000\u0000ak\u0003\u0002\u0001\u0005"+
    "bc\n\u0001\u0000\u0000cd\u0005 \u0000\u0000de\u0003 \u0010\u0000ef\u0003"+
    "\u0004\u0002\u0000fh\u0005!\u0000\u0000gi\u0003\u001c\u000e\u0000hg\u0001"+
    "\u0000\u0000\u0000hi\u0001\u0000\u0000\u0000ik\u0001\u0000\u0000\u0000"+
    "j;\u0001\u0000\u0000\u0000jA\u0001\u0000\u0000\u0000jG\u0001\u0000\u0000"+
    "\u0000jM\u0001\u0000\u0000\u0000jV\u0001\u0000\u0000\u0000j\\\u0001\u0000"+
    "\u0000\u0000jb\u0001\u0000\u0000\u0000kn\u0001\u0000\u0000\u0000lj\u0001"+
    "\u0000\u0000\u0000lm\u0001\u0000\u0000\u0000m\u0003\u0001\u0000\u0000"+
    "\u0000nl\u0001\u0000\u0000\u0000oq\u0005$\u0000\u0000pr\u0003 \u0010\u0000"+
    "qp\u0001\u0000\u0000\u0000qr\u0001\u0000\u0000\u0000r~\u0001\u0000\u0000"+
    "\u0000st\u0005*\u0000\u0000tu\u0005\u0006\u0000\u0000u~\u0003\u0002\u0001"+
    "\u0000vw\u0005*\u0000\u0000wx\u0007\u0004\u0000\u0000x~\u0003\u0002\u0001"+
    "\u0000yz\u0005*\u0000\u0000z{\u0007\u0000\u0000\u0000{~\u0003\u0002\u0001"+
    "\u0000|~\u0005*\u0000\u0000}o\u0001\u0000\u0000\u0000}s\u0001\u0000\u0000"+
    "\u0000}v\u0001\u0000\u0000\u0000}y\u0001\u0000\u0000\u0000}|\u0001\u0000"+
    "\u0000\u0000~\u0005\u0001\u0000\u0000\u0000\u007f\u0083\u0003\b\u0004"+
    "\u0000\u0080\u0083\u0003\f\u0006\u0000\u0081\u0083\u0003$\u0012\u0000"+
    "\u0082\u007f\u0001\u0000\u0000\u0000\u0082\u0080\u0001\u0000\u0000\u0000"+
    "\u0082\u0081\u0001\u0000\u0000\u0000\u0083\u0007\u0001\u0000\u0000\u0000"+
    "\u0084\u0085\u0005,\u0000\u0000\u0085\u0086\u0005\"\u0000\u0000\u0086"+
    "\u00a3\u0005#\u0000\u0000\u0087\u0088\u0005,\u0000\u0000\u0088\u0089\u0005"+
    "\"\u0000\u0000\u0089\u008e\u0003\u0002\u0001\u0000\u008a\u008b\u0005%"+
    "\u0000\u0000\u008b\u008d\u0003\u0002\u0001\u0000\u008c\u008a\u0001\u0000"+
    "\u0000\u0000\u008d\u0090\u0001\u0000\u0000\u0000\u008e\u008c\u0001\u0000"+
    "\u0000\u0000\u008e\u008f\u0001\u0000\u0000\u0000\u008f\u0091\u0001\u0000"+
    "\u0000\u0000\u0090\u008e\u0001\u0000\u0000\u0000\u0091\u0093\u0005#\u0000"+
    "\u0000\u0092\u0094\u0003\n\u0005\u0000\u0093\u0092\u0001\u0000\u0000\u0000"+
    "\u0093\u0094\u0001\u0000\u0000\u0000\u0094\u00a3\u0001\u0000\u0000\u0000"+
    "\u0095\u0096\u0005,\u0000\u0000\u0096\u0097\u0003\n\u0005\u0000\u0097"+
    "\u0098\u0005\"\u0000\u0000\u0098\u009d\u0003\u0002\u0001\u0000\u0099\u009a"+
    "\u0005%\u0000\u0000\u009a\u009c\u0003\u0002\u0001\u0000\u009b\u0099\u0001"+
    "\u0000\u0000\u0000\u009c\u009f\u0001\u0000\u0000\u0000\u009d\u009b\u0001"+
    "\u0000\u0000\u0000\u009d\u009e\u0001\u0000\u0000\u0000\u009e\u00a0\u0001"+
    "\u0000\u0000\u0000\u009f\u009d\u0001\u0000\u0000\u0000\u00a0\u00a1\u0005"+
    "#\u0000\u0000\u00a1\u00a3\u0001\u0000\u0000\u0000\u00a2\u0084\u0001\u0000"+
    "\u0000\u0000\u00a2\u0087\u0001\u0000\u0000\u0000\u00a2\u0095\u0001\u0000"+
    "\u0000\u0000\u00a3\t\u0001\u0000\u0000\u0000\u00a4\u00a5\u0007\u0005\u0000"+
    "\u0000\u00a5\u00a6\u0003\u0012\t\u0000\u00a6\u000b\u0001\u0000\u0000\u0000"+
    "\u00a7\u00ac\u0003\u000e\u0007\u0000\u00a8\u00a9\u0005 \u0000\u0000\u00a9"+
    "\u00aa\u0003 \u0010\u0000\u00aa\u00ab\u0005!\u0000\u0000\u00ab\u00ad\u0001"+
    "\u0000\u0000\u0000\u00ac\u00a8\u0001\u0000\u0000\u0000\u00ac\u00ad\u0001"+
    "\u0000\u0000\u0000\u00ad\u00af\u0001\u0000\u0000\u0000\u00ae\u00b0\u0003"+
    "\u001c\u000e\u0000\u00af\u00ae\u0001\u0000\u0000\u0000\u00af\u00b0\u0001"+
    "\u0000\u0000\u0000\u00b0\r\u0001\u0000\u0000\u0000\u00b1\u00b7\u0003\u001a"+
    "\r\u0000\u00b2\u00b4\u0005\u001e\u0000\u0000\u00b3\u00b5\u0003\u0014\n"+
    "\u0000\u00b4\u00b3\u0001\u0000\u0000\u0000\u00b4\u00b5\u0001\u0000\u0000"+
    "\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6\u00b8\u0005\u001f\u0000"+
    "\u0000\u00b7\u00b2\u0001\u0000\u0000\u0000\u00b7\u00b8\u0001\u0000\u0000"+
    "\u0000\u00b8\u00be\u0001\u0000\u0000\u0000\u00b9\u00ba\u0005\u001e\u0000"+
    "\u0000\u00ba\u00bb\u0003\u0014\n\u0000\u00bb\u00bc\u0005\u001f\u0000\u0000"+
    "\u00bc\u00be\u0001\u0000\u0000\u0000\u00bd\u00b1\u0001\u0000\u0000\u0000"+
    "\u00bd\u00b9\u0001\u0000\u0000\u0000\u00be\u000f\u0001\u0000\u0000\u0000"+
    "\u00bf\u00c0\u0007\u0006\u0000\u0000\u00c0\u00c5\u0003\u0012\t\u0000\u00c1"+
    "\u00c3\u0007\u0007\u0000\u0000\u00c2\u00c4\u0003\u0012\t\u0000\u00c3\u00c2"+
    "\u0001\u0000\u0000\u0000\u00c3\u00c4\u0001\u0000\u0000\u0000\u00c4\u00c6"+
    "\u0001\u0000\u0000\u0000\u00c5\u00c1\u0001\u0000\u0000\u0000\u00c5\u00c6"+
    "\u0001\u0000\u0000\u0000\u00c6\u0011\u0001\u0000\u0000\u0000\u00c7\u00ce"+
    "\u0005\"\u0000\u0000\u00c8\u00ca\u0003\u0018\f\u0000\u00c9\u00cb\u0005"+
    "%\u0000\u0000\u00ca\u00c9\u0001\u0000\u0000\u0000\u00ca\u00cb\u0001\u0000"+
    "\u0000\u0000\u00cb\u00cd\u0001\u0000\u0000\u0000\u00cc\u00c8\u0001\u0000"+
    "\u0000\u0000\u00cd\u00d0\u0001\u0000\u0000\u0000\u00ce\u00cc\u0001\u0000"+
    "\u0000\u0000\u00ce\u00cf\u0001\u0000\u0000\u0000\u00cf\u00d1\u0001\u0000"+
    "\u0000\u0000\u00d0\u00ce\u0001\u0000\u0000\u0000\u00d1\u00d2\u0005#\u0000"+
    "\u0000\u00d2\u0013\u0001\u0000\u0000\u0000\u00d3\u00da\u0003\u0016\u000b"+
    "\u0000\u00d4\u00d6\u0005%\u0000\u0000\u00d5\u00d7\u0003\u0016\u000b\u0000"+
    "\u00d6\u00d5\u0001\u0000\u0000\u0000\u00d6\u00d7\u0001\u0000\u0000\u0000"+
    "\u00d7\u00d9\u0001\u0000\u0000\u0000\u00d8\u00d4\u0001\u0000\u0000\u0000"+
    "\u00d9\u00dc\u0001\u0000\u0000\u0000\u00da\u00d8\u0001\u0000\u0000\u0000"+
    "\u00da\u00db\u0001\u0000\u0000\u0000\u00db\u0015\u0001\u0000\u0000\u0000"+
    "\u00dc\u00da\u0001\u0000\u0000\u0000\u00dd\u00e0\u0003\u0018\f\u0000\u00de"+
    "\u00df\u0007\b\u0000\u0000\u00df\u00e1\u0005&\u0000\u0000\u00e0\u00de"+
    "\u0001\u0000\u0000\u0000\u00e0\u00e1\u0001\u0000\u0000\u0000\u00e1\u0017"+
    "\u0001\u0000\u0000\u0000\u00e2\u00e6\u0003\u001a\r\u0000\u00e3\u00e6\u0005"+
    "&\u0000\u0000\u00e4\u00e6\u0003&\u0013\u0000\u00e5\u00e2\u0001\u0000\u0000"+
    "\u0000\u00e5\u00e3\u0001\u0000\u0000\u0000\u00e5\u00e4\u0001\u0000\u0000"+
    "\u0000\u00e6\u0019\u0001\u0000\u0000\u0000\u00e7\u00ea\u0005,\u0000\u0000"+
    "\u00e8\u00ea\u0003,\u0016\u0000\u00e9\u00e7\u0001\u0000\u0000\u0000\u00e9"+
    "\u00e8\u0001\u0000\u0000\u0000\u00ea\u001b\u0001\u0000\u0000\u0000\u00eb"+
    "\u00ed\u0003\u001e\u000f\u0000\u00ec\u00ee\u0003\"\u0011\u0000\u00ed\u00ec"+
    "\u0001\u0000\u0000\u0000\u00ed\u00ee\u0001\u0000\u0000\u0000\u00ee\u00f4"+
    "\u0001\u0000\u0000\u0000\u00ef\u00f1\u0003\"\u0011\u0000\u00f0\u00f2\u0003"+
    "\u001e\u000f\u0000\u00f1\u00f0\u0001\u0000\u0000\u0000\u00f1\u00f2\u0001"+
    "\u0000\u0000\u0000\u00f2\u00f4\u0001\u0000\u0000\u0000\u00f3\u00eb\u0001"+
    "\u0000\u0000\u0000\u00f3\u00ef\u0001\u0000\u0000\u0000\u00f4\u001d\u0001"+
    "\u0000\u0000\u0000\u00f5\u00f7\u0005\u001a\u0000\u0000\u00f6\u00f8\u0005"+
    "\u0002\u0000\u0000\u00f7\u00f6\u0001\u0000\u0000\u0000\u00f7\u00f8\u0001"+
    "\u0000\u0000\u0000\u00f8\u00f9\u0001\u0000\u0000\u0000\u00f9\u00fa\u0003"+
    " \u0010\u0000\u00fa\u001f\u0001\u0000\u0000\u0000\u00fb\u00fc\u0003\u0002"+
    "\u0001\u0000\u00fc!\u0001\u0000\u0000\u0000\u00fd\u00ff\u0005\u001b\u0000"+
    "\u0000\u00fe\u0100\u0005\u0002\u0000\u0000\u00ff\u00fe\u0001\u0000\u0000"+
    "\u0000\u00ff\u0100\u0001\u0000\u0000\u0000\u0100\u0101\u0001\u0000\u0000"+
    "\u0000\u0101\u0105\u0003*\u0015\u0000\u0102\u0103\u0005\u001b\u0000\u0000"+
    "\u0103\u0105\u0007\t\u0000\u0000\u0104\u00fd\u0001\u0000\u0000\u0000\u0104"+
    "\u0102\u0001\u0000\u0000\u0000\u0105#\u0001\u0000\u0000\u0000\u0106\u010a"+
    "\u0003&\u0013\u0000\u0107\u010a\u0003(\u0014\u0000\u0108\u010a\u0003*"+
    "\u0015\u0000\u0109\u0106\u0001\u0000\u0000\u0000\u0109\u0107\u0001\u0000"+
    "\u0000\u0000\u0109\u0108\u0001\u0000\u0000\u0000\u010a%\u0001\u0000\u0000"+
    "\u0000\u010b\u010f\u0005(\u0000\u0000\u010c\u010f\u0005\'\u0000\u0000"+
    "\u010d\u010f\u0005)\u0000\u0000\u010e\u010b\u0001\u0000\u0000\u0000\u010e"+
    "\u010c\u0001\u0000\u0000\u0000\u010e\u010d\u0001\u0000\u0000\u0000\u010f"+
    "\'\u0001\u0000\u0000\u0000\u0110\u0111\u0005&\u0000\u0000\u0111)\u0001"+
    "\u0000\u0000\u0000\u0112\u0116\u0005*\u0000\u0000\u0113\u0116\u0005+\u0000"+
    "\u0000\u0114\u0116\u0003&\u0013\u0000\u0115\u0112\u0001\u0000\u0000\u0000"+
    "\u0115\u0113\u0001\u0000\u0000\u0000\u0115\u0114\u0001\u0000\u0000\u0000"+
    "\u0116+\u0001\u0000\u0000\u0000\u0117\u0118\u0007\n\u0000\u0000\u0118"+
    "-\u0001\u0000\u0000\u0000)9>DJPSY_hjlq}\u0082\u008e\u0093\u009d\u00a2"+
    "\u00ac\u00af\u00b4\u00b7\u00bd\u00c3\u00c5\u00ca\u00ce\u00d6\u00da\u00e0"+
    "\u00e5\u00e9\u00ed\u00f1\u00f3\u00f7\u00ff\u0104\u0109\u010e\u0115";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
