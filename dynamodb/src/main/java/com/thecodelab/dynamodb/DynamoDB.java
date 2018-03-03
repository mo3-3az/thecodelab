package com.thecodelab.dynamodb;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.thecodelab.dynamodb.converter.DynamoDBItemConverter;
import com.thecodelab.dynamodb.converter.QuerySpecConverter;
import com.thecodelab.dynamodb.converter.ScanSpecConverter;
import com.thecodelab.dynamodb.creds.AwsCredentials;
import com.thecodelab.dynamodb.creds.AwsCredentialsProvider;
import com.thecodelab.dynamodb.exception.ContentDaoException;
import com.thecodelab.dynamodb.exception.DynamoDBItemConverterException;
import com.thecodelab.dynamodb.filters.Filters;
import com.thecodelab.dynamodb.model.ContentDao;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDB implements ContentDao {

    private static final String CONTENT_ATTRIBUTE_NAME = "content";
    private static final String PRIMARY_KEY_NAME = "key";
    private static final String VERSION_ATTRIBUTE_NAME = "version";

    private static final String PLACEHOLDER_K_VAL = "k_val";
    private static final String PLACEHOLDER_V_ID = "v_id";
    private static final String PLACEHOLDER_SK_VAL = "sk_val";
    private static final String PLACEHOLDER_SV_ID = "sv_id";

    private static final String JSON_KEY_CONTENT = CONTENT_ATTRIBUTE_NAME;
    private static final long DEFAULT_VERSION_VAL = 1;

    private String primaryKeyName;
    private String contentAttributeName;
    private String versionAttributeName;
    private String tableName;
    private boolean isVersioned;
    private DynamoDBItemConverter converter;
    private AmazonDynamoDBAsync dynamoDBAsync;

    private Table table;

    public DynamoDB(AwsCredentials credentials, String tableName) {
        this(credentials, tableName, true);
    }

    public DynamoDB(AwsCredentials credentials, String tableName, boolean isVersioned) {
        AwsCredentialsProvider awsCredentialsProvider = new AwsCredentialsProvider(credentials);
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(awsCredentialsProvider).withRegion(credentials.getRegion()).build();
        com.amazonaws.services.dynamodbv2.document.DynamoDB dynamoDB = new com.amazonaws.services.dynamodbv2.document.DynamoDB(client);
        table = dynamoDB.getTable(tableName);
        this.tableName = tableName;
        dynamoDBAsync = AmazonDynamoDBAsyncClient.asyncBuilder().withCredentials(awsCredentialsProvider).build();
        converter = new DynamoDBItemConverter();
        this.primaryKeyName = PRIMARY_KEY_NAME;
        this.contentAttributeName = CONTENT_ATTRIBUTE_NAME;
        this.versionAttributeName = VERSION_ATTRIBUTE_NAME;
        this.isVersioned = isVersioned;
    }

    ////////////////////////////////////////////////

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }

    public String getVersionAttributeName() {
        return versionAttributeName;
    }

    ///////////////////////////////////////////////////////

    @Override
    public String putContent(JsonObject val) throws ContentDaoException {
        try {
            Item item = prepareItemToCreate(val);
            table.putItem(item);
            return item.getString(primaryKeyName);
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to put content, table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    @Override
    public Long updContent(String key, JsonObject val) throws ContentDaoException {
        try {
            Item item = prepareItemToUpdate(key, val);
            table.putItem(item);
            return item.getLong(versionAttributeName);
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to update content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    @Override
    public JsonObject getContent(String key) throws ContentDaoException {
        try {
            QuerySpec spec = getQuerySpecForKey(key);
            ItemCollection<QueryOutcome> items = table.query(spec);
            if (items.iterator().hasNext()) {
                return new JsonObject(items.iterator().next().toJSON());
            } else {
                throw new ContentDaoException("Failed to get content with key: " + key + ", table: " + table.getTableName() + ", cause: no items with the specified key!");
            }
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to get content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    @Override
    public JsonObject getContent(String key, String version) throws ContentDaoException {
        try {
            QuerySpec spec = getQuerySpecForKeyAndVersion(key, version);
            ItemCollection<QueryOutcome> items = table.query(spec);
            IteratorSupport<Item, QueryOutcome> iterator = items.iterator();

            if (!iterator.hasNext()) {
                throw new ContentDaoException("Failed to get content with key: " + key + "and version: " + version + ", table: " + table.getTableName() + ", cause: no items with the specified key!");
            }

            Integer maxResultSize = spec.getMaxResultSize();
            if (maxResultSize != null && maxResultSize == 1) {
                return new JsonObject(iterator.next().toJSON());
            }

            JsonArray objects = new JsonArray();
            while (iterator.hasNext()) {
                objects.add(new JsonObject(iterator.next().toJSON()));
            }

            return new JsonObject().put(JSON_KEY_CONTENT, objects);
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to get content with key: " + key + "and version: " + version + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    @Override
    public JsonObject getContent(Filters filters) throws ContentDaoException {
        try {
            ItemCollection<ScanOutcome> scan = table.scan(filters.getScanSpec());
            JsonObject results = new JsonObject();
            IteratorSupport<Item, ScanOutcome> iterator = scan.iterator();
            JsonArray objects = new JsonArray();
            while (iterator.hasNext()) {
                objects.add(new JsonObject(iterator.next().toJSONPretty()));
            }

            results.put(JSON_KEY_CONTENT, objects);
            return results;
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to get content with filters: " + filters + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    @Override
    public void delContent(String key) throws ContentDaoException {
        JsonObject content = getContent(key, null);
        JsonArray items = content.getJsonArray(JSON_KEY_CONTENT);
        for (Object item : items) {
            delContent(key, ((JsonObject) item).getValue(versionAttributeName).toString());
        }
    }

    @Override
    public void delContent(String key, String version) throws ContentDaoException {
        try {
            table.deleteItem(new DeleteItemSpec().withPrimaryKey(primaryKeyName, key, versionAttributeName, Long.valueOf(version)));
        } catch (AmazonDynamoDBException e) {
            throw new ContentDaoException("Failed to delete content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }
    }

    //////////////////////////////////////////////////////////////////////////////////

    @Override
    public Future<String> putContentAsync(JsonObject val) {
        Future<String> stringFuture = Future.future();
        try {
            Item item = prepareItemToCreate(val);
            String keyValue = item.getString(primaryKeyName);
            PutItemRequest putItemRequest = new PutItemRequest();
            putItemRequest.setItem(InternalUtils.toAttributeValues(item));
            putItemRequest.setTableName(tableName);
            dynamoDBAsync.putItemAsync(putItemRequest, new AsyncHandler<PutItemRequest, PutItemResult>() {
                @Override
                public void onError(Exception e) {
                    stringFuture.fail(e);
                }

                @Override
                public void onSuccess(PutItemRequest request, PutItemResult putItemResult) {
                    stringFuture.complete(keyValue);
                }
            });

        } catch (AmazonDynamoDBException e) {
            stringFuture.fail("Failed to put content, table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return stringFuture;
    }

    @Override
    public Future<Long> updContentAsync(String key, JsonObject val) {
        Future<Long> longFuture = Future.future();
        try {
            Item item = prepareItemToUpdate(key, val);
            PutItemRequest putItemRequest = new PutItemRequest();
            putItemRequest.setItem(InternalUtils.toAttributeValues(item));
            putItemRequest.setTableName(tableName);
            dynamoDBAsync.putItemAsync(putItemRequest, new AsyncHandler<PutItemRequest, PutItemResult>() {
                @Override
                public void onError(Exception e) {
                    longFuture.fail(e);
                }

                @Override
                public void onSuccess(PutItemRequest request, PutItemResult putItemResult) {
                    longFuture.complete(item.getLong(versionAttributeName));
                }
            });
        } catch (AmazonDynamoDBException e) {
            longFuture.fail("Failed to update content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return longFuture;
    }

    @Override
    public Future<JsonObject> getContentAsync(String key) {
        Future<JsonObject> jsonObjectFuture = Future.future();
        try {
            QueryRequest queryRequest = new QuerySpecConverter(getQuerySpecForKey(key)).convertToQueryRequest(tableName);
            dynamoDBAsync.queryAsync(queryRequest, new AsyncHandler<QueryRequest, QueryResult>() {
                @Override
                public void onError(Exception e) {
                    jsonObjectFuture.fail(e);
                }

                @Override
                public void onSuccess(QueryRequest request, QueryResult queryResult) {
                    List<Map<String, AttributeValue>> items = queryResult.getItems();
                    if (items.isEmpty()) {
                        jsonObjectFuture.fail("Failed to get content with key: " + key + ", table: " + table.getTableName() + ", cause: no items with the apssed key!");
                        return;
                    }

                    try {
                        jsonObjectFuture.complete(converter.mapToJsonObject(items.get(0)));
                    } catch (DynamoDBItemConverterException e) {
                        jsonObjectFuture.fail(e);
                    }
                }
            });
        } catch (AmazonDynamoDBException e) {
            jsonObjectFuture.fail("Failed to get content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return jsonObjectFuture;
    }

    @Override
    public Future<JsonObject> getContentAsync(String key, String version) {
        Future<JsonObject> jsonObjectFuture = Future.future();
        try {
            QueryRequest queryRequest = new QuerySpecConverter(getQuerySpecForKeyAndVersion(key, version)).convertToQueryRequest(tableName);
            dynamoDBAsync.queryAsync(queryRequest, new AsyncHandler<QueryRequest, QueryResult>() {
                @Override
                public void onError(Exception e) {
                    jsonObjectFuture.fail(e);
                }

                @Override
                public void onSuccess(QueryRequest request, QueryResult queryResult) {
                    List<Map<String, AttributeValue>> items = queryResult.getItems();
                    if (items.isEmpty()) {
                        jsonObjectFuture.fail("Failed to get content with key: " + key + ", table: " + table.getTableName() + ", cause: no items with the apssed key!");
                        return;
                    }

                    try {
                        Integer maxResultSize = queryRequest.getLimit();
                        if (maxResultSize != null && maxResultSize == 1) {
                            jsonObjectFuture.complete(converter.mapToJsonObject(items.get(0)));
                        } else {
                            JsonArray objects = new JsonArray();
                            for (Map<String, AttributeValue> item : items) {
                                objects.add(converter.mapToJsonObject(item));
                            }
                            jsonObjectFuture.complete(new JsonObject().put(JSON_KEY_CONTENT, objects));
                        }
                    } catch (DynamoDBItemConverterException e) {
                        jsonObjectFuture.fail(e);
                    }
                }
            });
        } catch (AmazonDynamoDBException e) {
            jsonObjectFuture.fail("Failed to get content with key: " + key + ", version: " + version + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return jsonObjectFuture;
    }

    @Override
    public Future<JsonObject> getContentAsync(Filters filters) {
        Future<JsonObject> future = Future.future();
        try {
            dynamoDBAsync.scanAsync(new ScanSpecConverter(filters.getScanSpec()).convertToScanRequest(tableName), new AsyncHandler<ScanRequest, ScanResult>() {
                @Override
                public void onError(Exception e) {
                    future.fail(e);
                }

                @Override
                public void onSuccess(ScanRequest request, ScanResult scanResult) {
                    JsonArray objects = new JsonArray();
                    for (Map<String, AttributeValue> stringAttributeValueMap : scanResult.getItems()) {
                        try {
                            objects.add(converter.mapToJsonObject(stringAttributeValueMap));
                        } catch (DynamoDBItemConverterException e) {
                            future.fail(e);
                        }
                    }

                    future.complete(new JsonObject().put(JSON_KEY_CONTENT, objects));
                }
            });
        } catch (AmazonDynamoDBException e) {
            future.fail("Failed to get content with filters: " + filters + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return future;
    }

    @Override
    public Future<Void> delContentAsync(String key) {
        Future<Void> future = Future.future();
        try {
            getContentAsync(key, null).setHandler(jsonObjectAsyncResult -> {
                if (jsonObjectAsyncResult.failed()) {
                    future.fail("Failed to delete content with key: " + key + ", table: " + table.getTableName() + ", cause: " + jsonObjectAsyncResult.cause().getMessage());
                } else {
                    List<Future> futureList = new ArrayList<>();
                    JsonObject result = jsonObjectAsyncResult.result();
                    JsonArray items = result.getJsonArray(JSON_KEY_CONTENT);
                    items.forEach(item -> futureList.add(delContentAsync(key, ((JsonObject) item).getValue(versionAttributeName).toString())));

                    CompositeFuture.all(futureList).setHandler(event -> {
                        if (event.failed()) {
                            future.fail(event.cause());
                        } else {
                            future.complete();
                        }
                    });
                }
            });
        } catch (AmazonDynamoDBException e) {
            future.fail("Failed to delete content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return future;
    }

    @Override
    public Future<Void> delContentAsync(String key, String version) {
        Future<Void> voidFuture = Future.future();
        try {
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
            HashMap<String, AttributeValue> keyMap = new HashMap<>();
            keyMap.put(primaryKeyName, InternalUtils.toAttributeValue(key));
            HashMap<String, AttributeValue> versionMap = new HashMap<>();
            versionMap.put(versionAttributeName, InternalUtils.toAttributeValue(Long.valueOf(version)));
            deleteItemRequest.setKey(keyMap.entrySet().iterator().next(), versionMap.entrySet().iterator().next());
            deleteItemRequest.setTableName(tableName);
            dynamoDBAsync.deleteItemAsync(deleteItemRequest, new AsyncHandler<DeleteItemRequest, DeleteItemResult>() {
                @Override
                public void onError(Exception e) {
                    voidFuture.fail(e);
                }

                @Override
                public void onSuccess(DeleteItemRequest request, DeleteItemResult deleteItemResult) {
                    voidFuture.complete();
                }
            });
        } catch (AmazonDynamoDBException e) {
            voidFuture.fail("Failed to delete content with key: " + key + ", table: " + table.getTableName() + ", cause: " + e.getMessage());
        }

        return voidFuture;
    }

    /////////////////////////////////////////////////////

    private Item prepareItemToCreate(JsonObject val) {
        long currentTimeMillis = System.currentTimeMillis();
        String keyValue = String.valueOf(currentTimeMillis);
        Item item = new Item().withJSON(contentAttributeName, val.toString()).withPrimaryKey(primaryKeyName, keyValue);
        if (isVersioned) {
            item.withLong(versionAttributeName, currentTimeMillis);
        } else {
            item.withLong(versionAttributeName, DEFAULT_VERSION_VAL);
        }

        return item;
    }

    private Item prepareItemToUpdate(String key, JsonObject val) {
        long versionValue;
        Item item = new Item().withJSON(contentAttributeName, val.toString()).withPrimaryKey(primaryKeyName, key);
        if (isVersioned) {
            versionValue = System.currentTimeMillis();
        } else {
            versionValue = DEFAULT_VERSION_VAL;
        }
        item.withLong(versionAttributeName, versionValue);

        return item;
    }

    private QuerySpec getQuerySpecForKey(String key) {
        return new QuerySpec()
                .withScanIndexForward(false)
                .withMaxResultSize(1)
                .withKeyConditionExpression("#" + PLACEHOLDER_K_VAL + " = :" + PLACEHOLDER_V_ID)
                .withNameMap(new NameMap().with("#" + PLACEHOLDER_K_VAL, primaryKeyName))
                .withValueMap(new ValueMap().withString(":" + PLACEHOLDER_V_ID, key));
    }

    private QuerySpec getQuerySpecForKeyAndVersion(String key, String version) {
        QuerySpec spec = new QuerySpec();
        ValueMap valueMap = new ValueMap();
        NameMap nameMap = new NameMap();
        String keyConditionExpression;
        boolean versionPassed = StringUtils.isNotBlank(version);
        if (versionPassed) {
            keyConditionExpression = "#" + PLACEHOLDER_K_VAL + " = :" + PLACEHOLDER_V_ID + " AND #" + PLACEHOLDER_SK_VAL + " = :" + PLACEHOLDER_SV_ID;
            nameMap.with("#" + PLACEHOLDER_K_VAL, primaryKeyName).with("#" + PLACEHOLDER_SK_VAL, versionAttributeName);
            valueMap.withString(":" + PLACEHOLDER_V_ID, key);
            spec.withMaxResultSize(1);

            if (isVersioned) {
                valueMap.withLong(":" + PLACEHOLDER_SV_ID, Long.valueOf(version));
            } else {
                valueMap.withLong(":" + PLACEHOLDER_SV_ID, DEFAULT_VERSION_VAL);
            }
        } else {
            keyConditionExpression = "#" + PLACEHOLDER_K_VAL + " = :" + PLACEHOLDER_V_ID;
            nameMap.with("#" + PLACEHOLDER_K_VAL, primaryKeyName);
            valueMap.withString(":" + PLACEHOLDER_V_ID, key);
        }

        spec.withScanIndexForward(false).withKeyConditionExpression(keyConditionExpression).withNameMap(nameMap).withValueMap(valueMap);

        return spec;
    }
}
