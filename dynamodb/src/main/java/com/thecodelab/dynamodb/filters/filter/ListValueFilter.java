package com.thecodelab.dynamodb.filters.filter;

import com.thecodelab.dynamodb.filters.operator.ListValueBasedOperator;

import java.util.List;
import java.util.Map;

public class ListValueFilter extends ExpressibleFilter {
    private String attribute;
    private ListValueBasedOperator operator;
    private List<Object> values;

    public ListValueFilter(String attribute, ListValueBasedOperator operator, List<Object> value) {
        this.attribute = attribute;
        this.operator = operator;
        this.values = value;
    }

    @Override
    public String express(Map<String, Object> valueMap, Map<String, String> nameMap) {
        StringBuilder stringBuilder = new StringBuilder();
        int count = 1;
        for (Object val : values) {
            valueMap.put(VAL_PLACEHOLDER + (valueMap.size() + 1), val);
            stringBuilder.append(VAL_PLACEHOLDER + (valueMap.size()));
            if (count < values.size()) {
                stringBuilder.append(", ");
            }
            count++;
        }
        return attribute + " " + operator.getOperator() + "(" + stringBuilder.toString() + ")";
    }
}
