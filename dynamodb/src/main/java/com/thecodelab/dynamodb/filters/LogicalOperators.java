package com.thecodelab.dynamodb.filters;

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import java.util.ArrayList;

public class LogicalOperators {

    private Filters filters;
    private ArrayList<String> operators;

    public LogicalOperators(Filters filters) {
        this.filters = filters;
        operators = new ArrayList<>();
    }

    public Filters AND() {
        operators.add(" AND ");
        return filters;
    }

    public Filters OR() {
        operators.add(" OR ");
        return filters;
    }

    public ScanSpec getScanSpec() {
        return filters.getScanSpec();
    }

    public String get(int i) {
        return operators.get(i);
    }

    public Filters build() {
        return filters;
    }

}
