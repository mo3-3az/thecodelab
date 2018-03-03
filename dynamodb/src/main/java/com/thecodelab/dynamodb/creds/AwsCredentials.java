package com.thecodelab.dynamodb.creds;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;

        /*
         * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
         *
         * Licensed under the Apache License, Version 2.0 (the "License").
         * You may not use this file except in compliance with the License.
         * A copy of the License is located at
         *
         *  http://aws.amazon.com/apache2.0
         *
         * or in the "license" file accompanying this file. This file is distributed
         * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
         * express or implied. See the License for the specific language governing
         * permissions and limitations under the License.
         */

/**
 * Basic implementation of the AWSCredentials interface that allows callers to
 * pass in the AWS access key and secret access in the constructor.
 */
public class AwsCredentials implements AWSCredentials {

    private final String accessKey;
    private final String secretKey;
    private final String region;

    public AwsCredentials(String accessKey, String secretKey) {
        this(accessKey, secretKey, Regions.DEFAULT_REGION.getName());
    }


    public AwsCredentials(String accessKey, String secretKey, String region) {
        if (accessKey == null) {
            throw new IllegalArgumentException("Access key cannot be null.");
        }
        if (secretKey == null) {
            throw new IllegalArgumentException("Secret key cannot be null.");
        }
        if (region == null) {
            throw new IllegalArgumentException("Region cannot be null.");
        }

        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
    }

    public String getAWSAccessKeyId() {
        return accessKey;
    }

    public String getAWSSecretKey() {
        return secretKey;
    }

    public String getRegion() {
        return region;
    }
}