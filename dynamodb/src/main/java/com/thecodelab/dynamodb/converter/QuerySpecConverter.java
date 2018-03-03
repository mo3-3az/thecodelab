package com.thecodelab.dynamodb.converter;

import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;

import java.util.HashMap;
import java.util.Map;

public class QuerySpecConverter {
    private QuerySpec querySpec;

    public QuerySpecConverter(QuerySpec querySpec) {
        this.querySpec = querySpec;
    }

    public QueryRequest convertToQueryRequest(String tableName) {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setKeyConditionExpression(querySpec.getKeyConditionExpression());
        queryRequest.setExpressionAttributeNames(querySpec.getNameMap());
        queryRequest.setExpressionAttributeValues(convert(querySpec.getValueMap()));
        queryRequest.setLimit(querySpec.getMaxResultSize());
        queryRequest.setScanIndexForward(querySpec.isScanIndexForward());
        queryRequest.setTableName(tableName);
        return queryRequest;
    }

    private Map<String, AttributeValue> convert(Map<String, Object> valueMap) {
        HashMap<String, AttributeValue> convertedMap = new HashMap<>();
        valueMap.forEach((k, v) -> {
            convertedMap.put(k, InternalUtils.toAttributeValue(v));
        });

        return convertedMap;
    }
}
