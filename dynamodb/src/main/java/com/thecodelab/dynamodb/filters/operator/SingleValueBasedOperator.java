package com.thecodelab.dynamodb.filters.operator;

public enum SingleValueBasedOperator {
    EQUALS("="), NOT_EQUALS("<>"), LESS_OR_EQUAL("<="), GREATER_OR_EQUAL(">="), GREATER(">"), LESS("<");

    private String operator;

    SingleValueBasedOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
