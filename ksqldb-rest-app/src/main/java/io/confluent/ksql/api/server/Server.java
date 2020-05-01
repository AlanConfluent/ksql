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

package io.confluent.ksql.api.server;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.InternalEndpoints;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.JksOptions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the API server. On start-up it deploys multiple server verticles to spread
 * the load across available cores.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class Server {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(Server.class);

  private final Vertx vertx;
  private final KsqlRestConfig config;
  private final Endpoints endpoints;
  private final Map<PushQueryId, PushQueryHolder> queries = new ConcurrentHashMap<>();
  private final Set<HttpConnection> connections = new ConcurrentHashSet<>();
  private final int maxPushQueryCount;
  private final Set<String> deploymentIds = new HashSet<>();
  private final InternalEndpoints internalEndpoints;
  private final KsqlSecurityExtension securityExtension;
  private final Optional<AuthenticationPlugin> authenticationPlugin;
  private final ServerState serverState;
  private final List<URI> listeners = new ArrayList<>();
  private final List<URI> internalListeners = new ArrayList<>();
  private WorkerExecutor workerExecutor;

  public Server(final Vertx vertx, final KsqlRestConfig config,
      final Endpoints endpoints,
      final InternalEndpoints internalEndpoints,
      final KsqlSecurityExtension securityExtension,
      final Optional<AuthenticationPlugin> authenticationPlugin,
      final ServerState serverState) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.internalEndpoints = Objects.requireNonNull(internalEndpoints);
    this.securityExtension = Objects.requireNonNull(securityExtension);
    this.authenticationPlugin = Objects.requireNonNull(authenticationPlugin);
    this.serverState = Objects.requireNonNull(serverState);
    this.maxPushQueryCount = config.getInt(KsqlRestConfig.MAX_PUSH_QUERIES);
  }

  public synchronized void start() {
    if (!deploymentIds.isEmpty()) {
      throw new IllegalStateException("Already started");
    }
    final DeploymentOptions options = new DeploymentOptions()
        .setInstances(config.getInt(KsqlRestConfig.VERTICLE_INSTANCES));
    this.workerExecutor = vertx.createSharedWorkerExecutor("ksql-workers",
        config.getInt(KsqlRestConfig.WORKER_POOL_SIZE));
    log.debug("Deploying " + options.getInstances() + " instances of server verticle");

    final List<URI> listenUris = parseListeners(config);
    final Optional<URI> internalListenUri = parseInternalListener(config, listenUris);
    // If there's no special internal endpoint listen URI, then just bind them with the rest of the
    // endpoints.
    final Optional<InternalEndpoints> combinedInternalEndpointsOptional =
        Optional.ofNullable(!internalListenUri.isPresent() ? internalEndpoints : null);

    final int instances = config.getInt(KsqlRestConfig.VERTICLE_INSTANCES);

    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();
    deployFutures.addAll(setupPublicEndpoints(instances, listenUris,
        combinedInternalEndpointsOptional));
    deployFutures.addAll(setupInternalEndpoints(instances, internalListenUri));

    final CompletableFuture<Void> allDeployFuture = CompletableFuture.allOf(deployFutures
        .toArray(new CompletableFuture<?>[0]));

    try {
      allDeployFuture.get();
      for (CompletableFuture<String> deployFuture : deployFutures) {
        deploymentIds.add(deployFuture.get());
      }
    } catch (Exception e) {
      throw new KsqlException("Failed to start API server", e);
    }
    log.info("API server started");
  }

  private static List<CompletableFuture<String>> setupEndpointsCommon(
      final Vertx vertx,
      final int instances,
      final List<URI> listenUris,
      final Function<URI, AbstractServerVerticle> verticleFactory,
      final List<URI> listeners) {
    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();
    final Map<URI, URI> uris = new ConcurrentHashMap<>();
    for (URI listener : listenUris) {

      for (int i = 0; i < instances; i++) {
        final VertxCompletableFuture<String> vcf = new VertxCompletableFuture<>();
        final AbstractServerVerticle serverVerticle = verticleFactory.apply(listener);
        vertx.deployVerticle(serverVerticle, vcf);
        final int index = i;
        final CompletableFuture<String> deployFuture = vcf.thenApply(s -> {
          if (index == 0) {
            try {
              final URI uriWithPort = new URI(listener.getScheme(), null, listener.getHost(),
                  serverVerticle.actualPort(), null, null, null);
              uris.put(listener, uriWithPort);
            } catch (URISyntaxException e) {
              throw new KsqlException(e);
            }
          }
          return s;
        });
        deployFutures.add(deployFuture);
      }
    }
    CompletableFuture.allOf(deployFutures.toArray(new CompletableFuture<?>[0])).thenApply(n -> {
      for (URI uri : listenUris) {
        listeners.add(uris.get(uri));
      }
      return null;
    });
    return deployFutures;

  }

  private List<CompletableFuture<String>> setupPublicEndpoints(
      final int instances,
      final List<URI> listenUris,
      final Optional<InternalEndpoints> combinedInternalEndpointsOptional) {
    return setupEndpointsCommon(vertx, instances, listenUris, (listener) ->
        new ServerVerticle(endpoints,
            combinedInternalEndpointsOptional,
            createHttpServerOptions(config, listener.getHost(), listener.getPort(),
                listener.getScheme().equalsIgnoreCase("https")),
            this), listeners);
  }

  private List<CompletableFuture<String>> setupInternalEndpoints(
      final int instances,
      final Optional<URI> internalListenUri) {
    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();

    internalListenUri.ifPresent(listener -> {
      deployFutures.addAll(
          setupEndpointsCommon(vertx, instances, ImmutableList.of(listener), (l) ->
              new InternalServerVerticle(internalEndpoints,
                  createHttpServerOptions(config, l.getHost(), l.getPort(),
                      listener.getScheme().equalsIgnoreCase("https")),
                  this), internalListeners));
    });

    return deployFutures;
  }

  public synchronized void stop() {
    if (deploymentIds.isEmpty()) {
      throw new IllegalStateException("Not started");
    }
    if (workerExecutor != null) {
      workerExecutor.close();
    }
    final List<CompletableFuture<Void>> undeployFutures = new ArrayList<>();
    for (String deploymentID : deploymentIds) {
      final VertxCompletableFuture<Void> future = new VertxCompletableFuture<>();
      vertx.undeploy(deploymentID, future);
      undeployFutures.add(future);
    }
    try {
      CompletableFuture.allOf(undeployFutures.toArray(new CompletableFuture<?>[0])).get();
    } catch (Exception e) {
      throw new KsqlException("Failure in stopping API server", e);
    }
    deploymentIds.clear();
    log.info("API server stopped");
  }

  public WorkerExecutor getWorkerExecutor() {
    return workerExecutor;
  }

  synchronized void registerQuery(final PushQueryHolder query) {
    Objects.requireNonNull(query);
    if (queries.size() == maxPushQueryCount) {
      throw new KsqlApiException("Maximum number of push queries exceeded",
          ErrorCodes.ERROR_MAX_PUSH_QUERIES_EXCEEDED);
    }
    if (queries.putIfAbsent(query.getId(), query) != null) {
      // It should never happen
      // https://stackoverflow.com/questions/2513573/how-good-is-javas-uuid-randomuuid
      throw new IllegalStateException("Glitch in the matrix");
    }
  }

  Optional<PushQueryHolder> removeQuery(final PushQueryId queryId) {
    return Optional.ofNullable(queries.remove(queryId));
  }

  public Set<PushQueryId> getQueryIDs() {
    return new HashSet<>(queries.keySet());
  }

  KsqlSecurityExtension getSecurityExtension() {
    return securityExtension;
  }

  Optional<AuthenticationPlugin> getAuthenticationPlugin() {
    return authenticationPlugin;
  }

  ServerState getServerState() {
    return serverState;
  }

  public KsqlRestConfig getConfig() {
    return config;
  }

  void registerQueryConnection(final HttpConnection connection) {
    this.connections.add(Objects.requireNonNull(connection));
  }

  void removeQueryConnection(final HttpConnection connection) {
    connections.remove(Objects.requireNonNull(connection));
  }

  public int queryConnectionCount() {
    return connections.size();
  }

  public synchronized List<URI> getListeners() {
    return ImmutableList.copyOf(listeners);
  }

  public synchronized List<URI> getInternalListeners() {
    return ImmutableList.copyOf(internalListeners);
  }

  private static HttpServerOptions createHttpServerOptions(final KsqlRestConfig ksqlRestConfig,
      final String host, final int port, final boolean tls) {

    final HttpServerOptions options = new HttpServerOptions()
        .setHost(host)
        .setPort(port)
        .setReuseAddress(true)
        .setReusePort(true)
        .setIdleTimeout(60).setIdleTimeoutUnit(TimeUnit.SECONDS)
        .setPerMessageWebSocketCompressionSupported(true)
        .setPerFrameWebSocketCompressionSupported(true);

    if (tls) {
      options.setUseAlpn(true).setSsl(true);

      final String keyStorePath = ksqlRestConfig
          .getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
      final Password keyStorePassword = ksqlRestConfig
          .getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
      if (keyStorePath != null && !keyStorePath.isEmpty()) {
        options.setKeyStoreOptions(
            new JksOptions().setPath(keyStorePath).setPassword(keyStorePassword.value()));
      }

      final String trustStorePath = ksqlRestConfig
          .getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      final Password trustStorePassword = ksqlRestConfig
          .getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
      if (trustStorePath != null && !trustStorePath.isEmpty()) {
        options.setTrustStoreOptions(
            new JksOptions().setPath(trustStorePath).setPassword(trustStorePassword.value()));
      }

      options.setClientAuth(ksqlRestConfig.getClientAuth());
    }

    return options;
  }

  private static List<URI> parseListeners(final KsqlRestConfig config) {
    final List<String> sListeners = config.getList(KsqlRestConfig.LISTENERS_CONFIG);
    return parseListenerStrings(config, sListeners);
  }

  private static Optional<URI> parseInternalListener(
      final KsqlRestConfig config,
      final List<URI> listenUris
  ) {
    if (config.getString(KsqlRestConfig.INTERNAL_LISTENER_CONFIG) == null) {
      return Optional.empty();
    }
    final URI uri = parseListenerStrings(config,
        ImmutableList.of(config.getString(KsqlRestConfig.INTERNAL_LISTENER_CONFIG))).get(0);
    if (listenUris.contains(uri)) {
      return Optional.empty();
    } else {
      return Optional.of(uri);
    }
  }

  private static List<URI> parseListenerStrings(
      final KsqlRestConfig config,
      final List<String> stringListeners) {
    final List<URI> listeners = new ArrayList<>();
    for (String listenerName : stringListeners) {
      try {
        final URI uri = new URI(listenerName);
        final String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
          throw new ConfigException("Invalid URI scheme should be http or https: " + listenerName);
        }
        if ("https".equalsIgnoreCase(scheme)) {
          final String keyStoreLocation = config
              .getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
          if (keyStoreLocation == null || keyStoreLocation.isEmpty()) {
            throw new ConfigException("https listener specified but no keystore provided");
          }
        }
        listeners.add(uri);
      } catch (URISyntaxException e) {
        throw new ConfigException("Invalid listener URI: " + listenerName);
      }
    }
    return listeners;
  }

}
