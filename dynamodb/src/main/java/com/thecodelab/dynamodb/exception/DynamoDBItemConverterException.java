package com.thecodelab.dynamodb.exception;

import java.io.IOException;

public class DynamoDBItemConverterException extends Exception {
    public DynamoDBItemConverterException(String message) {
        super(message);
    }

    public DynamoDBItemConverterException(IOException e) {
        super(e);
    }
}
