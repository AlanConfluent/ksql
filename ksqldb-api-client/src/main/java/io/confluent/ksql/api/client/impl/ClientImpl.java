/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client.impl;

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.parsetools.RecordParser;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class ClientImpl implements Client {

  private final ClientOptions clientOptions;
  private final Vertx vertx;
  private final HttpClient httpClient;
  private final SocketAddress serverSocketAddress;
  private final String basicAuthHeader;
  private final boolean ownedVertx;

  public ClientImpl(final ClientOptions clientOptions) {
    this(clientOptions, Vertx.vertx(), true);
  }

  public ClientImpl(final ClientOptions clientOptions, final Vertx vertx) {
    this(clientOptions, vertx, false);
  }

  private ClientImpl(final ClientOptions clientOptions, final Vertx vertx,
      final boolean ownedVertx) {
    this.clientOptions = clientOptions.copy();
    this.vertx = vertx;
    this.ownedVertx = ownedVertx;
    this.httpClient = createHttpClient(vertx, clientOptions);
    this.basicAuthHeader = createBasicAuthHeader(clientOptions);
    this.serverSocketAddress =
        SocketAddress.inetSocketAddress(clientOptions.getPort(), clientOptions.getHost());
  }

  @Override
  public CompletableFuture<StreamedQueryResult> streamQuery(final String sql) {
    return streamQuery(sql, Collections.emptyMap());
  }

  @Override
  public CompletableFuture<StreamedQueryResult> streamQuery(
      final String sql,
      final Map<String, Object> properties
  ) {
    return makeQueryRequest(sql, properties, StreamQueryResponseHandler::new);
  }

  @Override
  public CompletableFuture<BatchedQueryResult> executeQuery(final String sql) {
    return executeQuery(sql, Collections.emptyMap());
  }

  @Override
  public CompletableFuture<BatchedQueryResult> executeQuery(
      final String sql,
      final Map<String, Object> properties
  ) {
    return makeQueryRequest(
        sql,
        properties,
        (context, recordParser, cf) -> new ExecuteQueryResponseHandler(
            context, recordParser, cf, clientOptions.getExecuteQueryMaxResultRows())
    );
  }

  @Override
  public CompletableFuture<Void> insertInto(
      final String streamName, final Map<String, Object> row) {
    return null; // not yet implemented
  }

  @Override
  public Publisher<InsertAck> streamInserts(
      final String streamName, final Publisher<List<Object>> insertsPublisher) {
    return null; // not yet implemented
  }

  @Override
  public CompletableFuture<Void> terminatePushQuery(final String queryId) {
    return makeCloseQueryRequest(queryId);
  }

  @Override
  public void close() {
    httpClient.close();
    if (ownedVertx) {
      vertx.close();
    }
  }

  @FunctionalInterface
  private interface ResponseHandlerSupplier<T> {
    QueryResponseHandler<T> get(Context ctx, RecordParser recordParser, CompletableFuture<T> cf);
  }

  private <T> CompletableFuture<T> makeQueryRequest(
      final String sql,
      final Map<String, Object> properties,
      final ResponseHandlerSupplier<T> responseHandlerSupplier
  ) {
    final JsonObject requestBody = new JsonObject().put("sql", sql).put("properties", properties);

    final CompletableFuture<T> cf = new CompletableFuture<>();
    makeRequest(
        "/query-stream",
        requestBody,
        cf,
        response -> handleQueryResponse(response, cf, responseHandlerSupplier)
    );

    return cf;
  }

  private CompletableFuture<Void> makeCloseQueryRequest(final String queryId) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();

    makeRequest(
        "/close-query",
        new JsonObject().put("queryId", queryId),
        cf,
        response -> handleCloseQueryResponse(response, cf)
    );

    return cf;
  }

  private <T> void makeRequest(
      final String path,
      final JsonObject requestBody,
      final CompletableFuture<T> cf,
      final Handler<HttpClientResponse> responseHandler) {
    HttpClientRequest request = httpClient.request(HttpMethod.POST,
        serverSocketAddress, clientOptions.getPort(), clientOptions.getHost(),
        path,
        responseHandler)
        .exceptionHandler(cf::completeExceptionally);
    if (clientOptions.isUseBasicAuth()) {
      request = configureBasicAuth(request);
    }
    request.end(requestBody.toBuffer());
  }

  private HttpClientRequest configureBasicAuth(final HttpClientRequest request) {
    return request.putHeader(AUTHORIZATION.toString(), basicAuthHeader);
  }

  private static <T> void handleQueryResponse(
      final HttpClientResponse response,
      final CompletableFuture<T> cf,
      final ResponseHandlerSupplier<T> responseHandlerSupplier) {
    if (response.statusCode() == OK.code()) {
      final RecordParser recordParser = RecordParser.newDelimited("\n", response);
      final QueryResponseHandler<T> responseHandler =
          responseHandlerSupplier.get(Vertx.currentContext(), recordParser, cf);

      recordParser.handler(responseHandler::handleBodyBuffer);
      recordParser.endHandler(responseHandler::handleBodyEnd);
      recordParser.exceptionHandler(responseHandler::handleException);
    } else {
      handleErrorResponse(response, cf);
    }
  }

  private static void handleCloseQueryResponse(
      final HttpClientResponse response,
      final CompletableFuture<Void> cf
  ) {
    if (response.statusCode() == OK.code()) {
      cf.complete(null);
    } else {
      handleErrorResponse(response, cf);
    }
  }

  private static <T> void handleErrorResponse(
      final HttpClientResponse response,
      final CompletableFuture<T> cf
  ) {
    response.bodyHandler(buffer -> {
      final JsonObject errorResponse = buffer.toJsonObject();
      cf.completeExceptionally(new KsqlRestClientException(String.format(
          "Received %d response from server: %s. Error code: %d",
          response.statusCode(),
          errorResponse.getString("message"),
          errorResponse.getInteger("error_code")
      )));
    });
  }

  private static HttpClient createHttpClient(final Vertx vertx, final ClientOptions clientOptions) {
    HttpClientOptions options = new HttpClientOptions()
        .setSsl(clientOptions.isUseTls())
        .setUseAlpn(true)
        .setProtocolVersion(HttpVersion.HTTP_2)
        .setVerifyHost(clientOptions.isVerifyHost())
        .setDefaultHost(clientOptions.getHost())
        .setDefaultPort(clientOptions.getPort());
    if (clientOptions.isUseTls() && !clientOptions.getTrustStore().isEmpty()) {
      options = options.setTrustStoreOptions(
          new JksOptions()
              .setPath(clientOptions.getTrustStore())
              .setPassword(clientOptions.getTrustStorePassword())
      );
    }
    if (clientOptions.isUseClientAuth()) {
      options = options.setKeyStoreOptions(
          new JksOptions()
              .setPath(clientOptions.getKeyStore())
              .setPassword(clientOptions.getKeyStorePassword())
      );
    }
    return vertx.createHttpClient(options);
  }

  private static String createBasicAuthHeader(final ClientOptions clientOptions) {
    if (!clientOptions.isUseBasicAuth()) {
      return "";
    }

    final String creds = clientOptions.getBasicAuthUsername()
        + ":"
        + clientOptions.getBasicAuthPassword();
    final String base64creds =
        Base64.getEncoder().encodeToString(creds.getBytes(Charset.defaultCharset()));
    return "Basic " + base64creds;
  }
}