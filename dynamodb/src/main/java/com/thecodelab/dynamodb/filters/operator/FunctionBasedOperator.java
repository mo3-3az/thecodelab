package com.thecodelab.dynamodb.filters.operator;

public enum FunctionBasedOperator {
    EXISTS("attribute_exists"), NOT_EXISTS("attribute_not_exists"), SIZE("size");

    private String operator;

    FunctionBasedOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
