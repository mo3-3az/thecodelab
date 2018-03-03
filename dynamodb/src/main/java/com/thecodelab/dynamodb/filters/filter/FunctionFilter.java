package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.FunctionBasedOperator;

import java.util.Map;

public class FunctionFilter extends ExpressibleFilter {
    String attribute;
    protected FunctionBasedOperator operator;

    public FunctionFilter(String attribute, FunctionBasedOperator operator) {
        this.attribute = attribute;
        this.operator = operator;
    }

    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        return operator.getOperator() + "(" + getNestedAttributeName(attribute, nameMap) + ")";
    }
}
