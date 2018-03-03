package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.FunctionBasedOperator;
import com.thecodelab.dynamodb.filters.operator.SingleValueBasedOperator;

import java.util.Map;

public class SizeFunctionFilter extends FunctionFilter {
    private Integer value;
    private SingleValueBasedOperator singleValueBasedOperator;

    public SizeFunctionFilter(String attribute, FunctionBasedOperator operator, Integer value, SingleValueBasedOperator singleValueBasedOperator) {
        super(attribute, operator);
        this.value = value;
        this.singleValueBasedOperator = singleValueBasedOperator;
    }


    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), value);
        nameMap.put(ATT_PLACEHOLDER + (nameMap.size() + 1), attribute);
        return operator.getOperator() + "(" + ATT_PLACEHOLDER + nameMap.size() + ") " + singleValueBasedOperator.getOperator() + " " + VAL_PLACEHOLDER + valueMap.size();
    }
}
