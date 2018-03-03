package com.thecodelab.dynamodb.filters.operator;

public enum ListValueBasedOperator {
    IN("IN");

    private String operator;

    ListValueBasedOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
