package com.thecodelab.dynamodb.filters.operator;

public enum DoubleValueBasedOperator {
    BETWEEN("BETWEEN");

    private String operator;

    DoubleValueBasedOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
