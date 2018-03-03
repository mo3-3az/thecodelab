package com.thecodelab.dynamodb.filters.filter;

import java.util.Map;

public interface Expressible {
    String express(Map<String, Object> valueMap, Map<String, String> nameMap);
}
