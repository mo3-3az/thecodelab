package com.thecodelab.dynamodb.model;

import com.thecodelab.dynamodb.exception.ContentDaoException;
import com.thecodelab.dynamodb.filters.Filters;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface ContentDao {

    String putContent(JsonObject val) throws ContentDaoException;

    Long updContent(String key, JsonObject val) throws ContentDaoException;

    void delContent(String key) throws ContentDaoException;

    void delContent(String key, String version) throws ContentDaoException;

    JsonObject getContent(String key) throws ContentDaoException;

    JsonObject getContent(String key, String version) throws ContentDaoException;

    JsonObject getContent(Filters filters) throws ContentDaoException;

    Future<String> putContentAsync(JsonObject val);

    Future<Long> updContentAsync(String key, JsonObject val);

    Future<Void> delContentAsync(String key);

    Future<Void> delContentAsync(String key, String version);

    Future<JsonObject> getContentAsync(String key);

    Future<JsonObject> getContentAsync(String key, String version);

    Future<JsonObject> getContentAsync(Filters filters);
}
