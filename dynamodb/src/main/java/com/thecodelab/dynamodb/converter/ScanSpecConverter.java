package com.thecodelab.dynamodb.converter;

import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import java.util.HashMap;
import java.util.Map;

public class ScanSpecConverter {
    private ScanSpec scanSpec;

    public ScanSpecConverter(ScanSpec scanSpec) {
        this.scanSpec = scanSpec;
    }

    public ScanRequest convertToScanRequest(String tableName) {
        ScanRequest scanRequest = new ScanRequest();
        scanRequest.setExpressionAttributeNames(scanSpec.getNameMap());
        Map<String, Object> valueMap = scanSpec.getValueMap();
        if (valueMap != null) {
            scanRequest.setExpressionAttributeValues(convert(valueMap));
        }
        scanRequest.setLimit(scanSpec.getMaxResultSize());
        scanRequest.setTableName(tableName);
        scanRequest.setFilterExpression(scanSpec.getFilterExpression());
        return scanRequest;
    }

    private Map<String, AttributeValue> convert(Map<String, Object> valueMap) {
        HashMap<String, AttributeValue> convertedMap = new HashMap<>();
        valueMap.forEach((k, v) -> convertedMap.put(k, InternalUtils.toAttributeValue(v)));
        return convertedMap;
    }
}
