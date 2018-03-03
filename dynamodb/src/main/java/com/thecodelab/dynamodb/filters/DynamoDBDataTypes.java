package com.thecodelab.dynamodb.filters;

public enum DynamoDBDataTypes {
    BINARY_SET("BS"), BINARY("B"), MAP("M"), STRING_SET("SS"), BOOLEAN("BOOL"), NULL("NULL"), NUMBER("N"), STRING("S"), NUMBER_SET("NS"), LIST("L");

    private String type;

    DynamoDBDataTypes(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
