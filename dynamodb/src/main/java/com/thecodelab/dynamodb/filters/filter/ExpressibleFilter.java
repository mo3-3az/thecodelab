package com.thecodelab.dynamodb.filters.filter;

import java.util.Map;

public abstract class ExpressibleFilter implements Expressible {
    protected static final String VAL_PLACEHOLDER = ":val";
    protected static final String ATT_PLACEHOLDER = "#att";

    protected String getNestedAttributeName(String attribute, Map<String, String> nameMap) {
        String[] splits = attribute.split("\\.");
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < splits.length; i++) {
            nameMap.put(ATT_PLACEHOLDER + (nameMap.size() + 1), splits[i]);
            stringBuilder.append(ATT_PLACEHOLDER).append(nameMap.size());
            if (i < splits.length - 1) {
                stringBuilder.append(".");
            }
        }

        return stringBuilder.toString();
    }
}
