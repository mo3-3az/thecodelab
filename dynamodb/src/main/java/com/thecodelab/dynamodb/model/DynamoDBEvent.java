package com.thecodelab.dynamodb.model;

public enum DynamoDBEvent {
    INSERT,MODIFY,REMOVE;

    public static boolean isInsert(String eventName) {
        return INSERT.name().equalsIgnoreCase(eventName);
    }

    public static boolean isModify(String eventName) {
        return MODIFY.name().equalsIgnoreCase(eventName);
    }

    public static boolean isRemove(String eventName) {
        return REMOVE.name().equalsIgnoreCase(eventName);
    }
    }
