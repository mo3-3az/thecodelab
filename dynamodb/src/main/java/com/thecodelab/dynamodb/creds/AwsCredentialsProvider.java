package com.thecodelab.dynamodb.creds;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

public class AwsCredentialsProvider implements AWSCredentialsProvider {

    private final AWSCredentials aWSCredentials;

    public AwsCredentialsProvider(AWSCredentials aWSCredentials) {
        this.aWSCredentials = aWSCredentials;
    }

    public AWSCredentials getCredentials() {
        return aWSCredentials;
    }

    public void refresh() {
    }

}
