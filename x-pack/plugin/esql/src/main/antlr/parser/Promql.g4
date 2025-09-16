/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Promql;

singleExpression
    : expression EOF
    ;

// operator precedence defined in Promql at
// https://prometheus.io/docs/prometheus/latest/querying/operators/#binary-operator-precedence

expression
    : <assoc=right> left=expression op=CARET modifier? right=expression                         #arithmeticBinary
    | operator=(PLUS | MINUS) expression                                                        #arithmeticUnary
    | left=expression op=(ASTERISK | PERCENT | SLASH) modifier? right=expression                #arithmeticBinary
    | left=expression op=(MINUS | PLUS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=(EQ | NEQ | GT | GTE | LT | LTE) BOOL? modifier? right=expression      #arithmeticBinary
    | left=expression op=(AND | UNLESS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=OR modifier? right=expression                                          #arithmeticBinary
    | value                                                                                     #valueExpression
    | LP expression RP                                                                          #parenthesized
    | expression LSB range=TIME_VALUE (':'|IDENTIFIER) RSB evaluation?                          #subquery
    ;

value
    : function
    | selector
    | constant
    ;

function
    : IDENTIFIER LP RP
    | IDENTIFIER LP expression (COMMA expression)* RP functionModifier?
    | IDENTIFIER functionModifier LP expression (COMMA expression)* RP
    ;

functionModifier
    : (BY | WITHOUT) labelList
    ;

selector
    : seriesMatcher (LSB duration RSB)? evaluation?
    ;

seriesMatcher
    : identifier (LCB labels? RCB)?
    | LCB labels RCB
    ;

modifier
    : (IGNORING | ON) modifierLabels=labelList (group=(GROUP_LEFT | GROUP_RIGHT) groupLabels=labelList?)?
    ;

// NB: PromQL explicitly allows a trailing comma for label enumeration
// both inside aggregation functions and metric labels.
labelList
    : LP (identifier COMMA?)* RP
    ;

labels
    :  label (COMMA label?)*
    ;

label
    : identifier kind=(LABEL_EQ | NEQ | LABEL_RGX | LABEL_RGX_NEQ) STRING
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

evaluation
    : offset at?
    | at offset?
    ;

offset
    : OFFSET MINUS? duration
    ;

// do timeunit validation and break-down inside the parser
// this helps deal with ambiguities for multi-unit declarations (1d3m)
// and give better error messages
duration
    :  TIME_VALUE
    ;

at
    : AT MINUS? number
    | AT (AT_START | AT_END)
    ;

constant
    : number
    | string
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    | HEXADECIMAL    #hexLiteral
    ;

string
    : STRING
    ;

// declared tokens that can be used without special escaping
// in PromQL this applies to all keywords
nonReserved
    : AND
    | BOOL
    | BY
    | GROUP_LEFT
    | GROUP_RIGHT
    | IGNORING
    | OFFSET
    | OR
    | ON
    | UNLESS
    | WITHOUT
    ;
