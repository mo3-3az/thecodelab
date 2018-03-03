package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.DoubleValueBasedOperator;

import java.util.Map;

public class DoubleValueFilter extends ExpressibleFilter {
    private String attribute;
    private DoubleValueBasedOperator operator;
    private Object value1;
    private Object value2;

    public DoubleValueFilter(String attribute, DoubleValueBasedOperator operator, Object value1, Object value2) {
        this.attribute = attribute;
        this.operator = operator;
        this.value1 = value1;
        this.value2 = value2;
    }

    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), value1);
        valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), value2);
        return attribute + " " + operator.getOperator() + " " + VAL_PLACEHOLDER + (valueMap.size() - 1) + " AND " + VAL_PLACEHOLDER + (valueMap.size());
    }
}