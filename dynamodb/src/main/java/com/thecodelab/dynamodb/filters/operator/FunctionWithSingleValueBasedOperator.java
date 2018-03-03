package com.thecodelab.dynamodb.filters.operator;

public enum FunctionWithSingleValueBasedOperator {
    TYPE("attribute_type"), CONTAINS("contains"), BEGINS_WITH("begins_with");

    private String operator;

    FunctionWithSingleValueBasedOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
