package com.thecodelab.dynamodb.filters;

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.thecodelab.dynamodb.filters.filter.*;
import com.thecodelab.dynamodb.filters.operator.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filters {

    private List<Expressible> filtersList;
    private LogicalOperators logicalOperators;
    private StringBuilder filterExpression;
    private Map<String, Object> valueMap;
    private Map<String, String> nameMap;

    private ScanSpec scanSpec;

    public Filters() {
        filtersList = new ArrayList<>();
        logicalOperators = new LogicalOperators(this);
        filterExpression = new StringBuilder();
        valueMap = new HashMap<>();
        nameMap = new HashMap<>();
    }

    public ScanSpec getScanSpec() {
        prepareScanSpec();
        return scanSpec;
    }


    public Filters build() {
        return this;
    }

    private void prepareScanSpec() {
        if (filtersList.isEmpty()) {
            scanSpec = new ScanSpec();
            return;
        }

        int size = filtersList.size();
        for (int i = 0; i < size; i++) {
            filterExpression.append(filtersList.get(i).express(valueMap, nameMap));
            if (i < size - 1) {
                filterExpression.append(logicalOperators.get(i));
            }
            filterExpression.append("\n");
        }

        scanSpec = new ScanSpec()
                .withFilterExpression(filterExpression.toString());

        if (!nameMap.isEmpty()) {
            scanSpec.withNameMap(nameMap);
        }

        if (!valueMap.isEmpty()) {
            scanSpec.withValueMap(valueMap);
        }
    }

    public LogicalOperators between(String attribute, Object val1, Object val2) {
        filtersList.add(new DoubleValueFilter(attribute, DoubleValueBasedOperator.BETWEEN, val1, val2));
        return logicalOperators;
    }

    public LogicalOperators exists(String attribute) {
        filtersList.add(new FunctionFilter(attribute, FunctionBasedOperator.EXISTS));
        return logicalOperators;
    }

    public LogicalOperators doesNotExist(String attribute) {
        filtersList.add(new FunctionFilter(attribute, FunctionBasedOperator.NOT_EXISTS));
        return logicalOperators;
    }

    public LogicalOperators sizeGreater(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.GREATER, value);
    }

    public LogicalOperators sizeLess(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.LESS, value);
    }

    public LogicalOperators sizeGreaterOrEqual(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.GREATER_OR_EQUAL, value);
    }

    public LogicalOperators sizeLessOrEqual(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.LESS_OR_EQUAL, value);
    }

    public LogicalOperators sizeEqual(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.EQUALS, value);
    }

    public LogicalOperators sizeNotEqual(String attribute, Integer value) {
        return size(attribute, SingleValueBasedOperator.NOT_EQUALS, value);
    }

    private LogicalOperators size(String attribute, SingleValueBasedOperator operator, Integer value) {
        filtersList.add(new SizeFunctionFilter(attribute, FunctionBasedOperator.SIZE, value, operator));
        return logicalOperators;
    }

    public LogicalOperators beginsWith(String attribute, String val) {
        filtersList.add(new FunctionWithValueFilter(attribute, FunctionWithSingleValueBasedOperator.BEGINS_WITH, val));
        return logicalOperators;
    }

    public LogicalOperators contains(String attribute, String val) {
        filtersList.add(new FunctionWithValueFilter(attribute, FunctionWithSingleValueBasedOperator.CONTAINS, val));
        return logicalOperators;
    }

    public LogicalOperators typeBinary(String attribute) {
        return type(attribute, DynamoDBDataTypes.BINARY);
    }

    public LogicalOperators typeBinarySet(String attribute) {
        return type(attribute, DynamoDBDataTypes.BINARY_SET);
    }

    public LogicalOperators typeBoolean(String attribute) {
        return type(attribute, DynamoDBDataTypes.BOOLEAN);
    }

    public LogicalOperators typeList(String attribute) {
        return type(attribute, DynamoDBDataTypes.LIST);
    }

    public LogicalOperators typeMap(String attribute) {
        return type(attribute, DynamoDBDataTypes.MAP);
    }

    public LogicalOperators typeNull(String attribute) {
        return type(attribute, DynamoDBDataTypes.NULL);
    }

    public LogicalOperators typeNumber(String attribute) {
        return type(attribute, DynamoDBDataTypes.NUMBER);
    }

    public LogicalOperators typeNumberSet(String attribute) {
        return type(attribute, DynamoDBDataTypes.NUMBER_SET);
    }

    public LogicalOperators typeString(String attribute) {
        return type(attribute, DynamoDBDataTypes.STRING);
    }

    public LogicalOperators typeStringSet(String attribute) {
        return type(attribute, DynamoDBDataTypes.STRING_SET);
    }

    private LogicalOperators type(String attribute, DynamoDBDataTypes dynamoDBDataTypes) {
        filtersList.add(new FunctionWithValueFilter(attribute, FunctionWithSingleValueBasedOperator.TYPE, dynamoDBDataTypes.getType()));
        return logicalOperators;
    }

    public LogicalOperators in(String attribute, List<Object> valsList) {
        filtersList.add(new ListValueFilter(attribute, ListValueBasedOperator.IN, valsList));
        return logicalOperators;
    }

    public LogicalOperators doesEqual(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.EQUALS, val));
        return logicalOperators;
    }

    public LogicalOperators notEqual(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.NOT_EQUALS, val));
        return logicalOperators;
    }

    public LogicalOperators greater(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.GREATER, val));
        return logicalOperators;
    }

    public LogicalOperators greaterOrEqual(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.GREATER_OR_EQUAL, val));
        return logicalOperators;
    }

    public LogicalOperators less(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.LESS, val));
        return logicalOperators;
    }

    public LogicalOperators lessOrEqual(String attribute, Object val) {
        filtersList.add(new SingleValueFilter(attribute, SingleValueBasedOperator.LESS_OR_EQUAL, val));
        return logicalOperators;
    }

    @Override
    public String toString() {
        return filterExpression.toString();
    }
}
