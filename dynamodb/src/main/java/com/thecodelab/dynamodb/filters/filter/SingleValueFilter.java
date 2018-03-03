package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.SingleValueBasedOperator;

import java.util.Map;

public class SingleValueFilter extends ExpressibleFilter {
    private String attribute;
    private SingleValueBasedOperator operator;
    private Object value;

    public SingleValueFilter(String attribute, SingleValueBasedOperator operator, Object value) {
        this.attribute = attribute;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), value);
        return attribute + " " + operator.getOperator() + " " + VAL_PLACEHOLDER + valueMap.size();
    }

}
