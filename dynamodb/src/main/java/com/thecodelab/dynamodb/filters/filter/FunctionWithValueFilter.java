package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.FunctionWithSingleValueBasedOperator;

import java.util.Map;

public class FunctionWithValueFilter extends ExpressibleFilter {
    private String attribute;
    private FunctionWithSingleValueBasedOperator operator;
    private Object value;

    public FunctionWithValueFilter(String attribute, FunctionWithSingleValueBasedOperator operator, Object value) {
        this.attribute = attribute;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), value);
        return operator.getOperator() + "(" + getNestedAttributeName(attribute, nameMap) + ", " + VAL_PLACEHOLDER + valueMap.size() + ")";
    }
}
