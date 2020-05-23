package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.sendClusterStatusRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.JAAS_KAFKA_PROPS_NAME;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.HostAliasResolver;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@Category({IntegrationTest.class})
@RunWith(Enclosed.class)
public class SystemAuthenticationFunctionalTest {

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("internal.example.com",
      8188);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("internal.example.com",
      8189);
  private static final URI LISTENER0 = URI.create("https://external.example.com:8088");
  private static final Map<String, String> KEYSTORE_PROPS = internalKeyStoreProps();
  private static final Optional<HostAliasResolver> HOST_ALIAS_RESOLVER =
      Optional.of(new LocalhostResolver());

  private static final Map<String, Object> JASS_AUTH_CONFIG = ImmutableMap.<String, Object>builder()
      .put("authentication.method", "BASIC")
      .put("authentication.roles", "**")
      // Reuse the Kafka JAAS config for KSQL authentication which has the same valid users
      .put("authentication.realm", JAAS_KAFKA_PROPS_NAME)
      .put(
          KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS,
          MockKsqlSecurityExtension.class.getName()
      )
      .build();

  private static final Map<String, Object> COMMON_CONFIG = ImmutableMap.<String, Object>builder()
      .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 200)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 1000)
      .put(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
      .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      .putAll(internalKeyStoreProps())
      .build();

  private static Map<String, String> internalKeyStoreProps() {
    Map<String, String> keyStoreProps = ServerKeyStore.keyStoreMultipleCertsProps();
    Map<String, String> trustStoreProps = ClientTrustStore.trustStoreMultipleCertsProps();
    return ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        keyStoreProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        keyStoreProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        trustStoreProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        trustStoreProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    );
  }

  private static final BasicCredentials USER1 = BasicCredentials.of(
      VALID_USER1.username,
      VALID_USER1.password
  );

  private static void commonClassSetup(final IntegrationTestHarness TEST_HARNESS,
      final TestKsqlRestApp REST_APP_0) {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(KEYSTORE_PROPS, LISTENER0, true,
        PAGE_VIEWS_PROVIDER, Optional.of(USER1), HOST_ALIAS_RESOLVER);
    RestIntegrationTestUtil.makeKsqlRequest(
        KEYSTORE_PROPS,
        LISTENER0,
        true,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";",
        Optional.of(USER1),
        HOST_ALIAS_RESOLVER
    );
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class MutualAuth {
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(HOST_ALIAS_RESOLVER)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "https://0.0.0.0:8088")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://internal.example.com:8188")
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:8188")
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED)
        .withProperties(COMMON_CONFIG)
        .withProperties(JASS_AUTH_CONFIG)
        .build();

    private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(HOST_ALIAS_RESOLVER)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "https://0.0.0.0:8089")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://internal.example.com:8189")
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:8189")
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED)
        .withProperties(COMMON_CONFIG)
        .withProperties(JASS_AUTH_CONFIG)
        .build();

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
        .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
        .around(TEST_HARNESS)
        .around(REST_APP_0)
        .around(REST_APP_1);

    @Mock
    private KsqlAuthorizationProvider authorizationProvider;

    @BeforeClass
    public static void setUpClass() {
      KsqlAuthorizationProvider staticAuthorizationProvider =
          Mockito.mock(KsqlAuthorizationProvider.class);
      MockKsqlSecurityExtension.setAuthorizationProvider(staticAuthorizationProvider);
      allowAccess(staticAuthorizationProvider, USER1, "POST", "/ksql");
      commonClassSetup(TEST_HARNESS, REST_APP_0);
    }

    @Before
    public void setUp() {
      MockKsqlSecurityExtension.setAuthorizationProvider(authorizationProvider);
    }


    @Test(timeout = 60000)
    public void shouldHeartbeatSuccessfully() throws InterruptedException {
      // Given:
      allowAccess(authorizationProvider, USER1, "GET", "/clusterStatus");
      waitForClusterToBeDiscovered(REST_APP_0, 2, KEYSTORE_PROPS, LISTENER0, true, Optional.of(USER1),
          HOST_ALIAS_RESOLVER);

      // This ensures that we can't hit the initial optimistic alive status
      Thread.sleep(2000);

      // When:
      waitForRemoteServerToChangeStatus(
          host0, HighAvailabilityTestUtil::remoteServerIsUp, KEYSTORE_PROPS, LISTENER0, true,
          Optional.of(USER1), HOST_ALIAS_RESOLVER);
      waitForRemoteServerToChangeStatus(
          host1, HighAvailabilityTestUtil::remoteServerIsUp, KEYSTORE_PROPS, LISTENER0, true,
          Optional.of(USER1), HOST_ALIAS_RESOLVER);
      ClusterStatusResponse response = sendClusterStatusRequest(KEYSTORE_PROPS,
          LISTENER0, true, Optional.of(USER1), HOST_ALIAS_RESOLVER);

      // Then:
      assertThat(response.getClusterStatus().get(host0).getHostAlive(), is(true));
      assertThat(response.getClusterStatus().get(host1).getHostAlive(), is(true));
      verify(authorizationProvider, never())
          .checkEndpointAccess(argThat(new PrincipalMatcher(USER1)), any(),
              not(eq("/clusterStatus")));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class HttpsNoMutualAuth {
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(HOST_ALIAS_RESOLVER)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "https://0.0.0.0:8088")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://internal.example.com:8188")
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:8188")
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE)
        .withProperties(COMMON_CONFIG)
        .build();

    private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(HOST_ALIAS_RESOLVER)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "https://0.0.0.0:8089")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://internal.example.com:8189")
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:8189")
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE)
        .withProperties(COMMON_CONFIG)
        .build();

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
        .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
        .around(TEST_HARNESS)
        .around(REST_APP_0)
        .around(REST_APP_1);

    @Mock
    private KsqlAuthorizationProvider authorizationProvider;

    @BeforeClass
    public static void setUpClass() {
      commonClassSetup(TEST_HARNESS, REST_APP_0);
    }

    @Test(timeout = 60000)
    public void shouldHeartbeatSuccessfully() throws InterruptedException {
      // Given:
      waitForClusterToBeDiscovered(REST_APP_0, 2, KEYSTORE_PROPS, LISTENER0, true, Optional.of(USER1),
          HOST_ALIAS_RESOLVER);

      // This ensures that we can't hit the initial optimistic alive status
      Thread.sleep(2000);

      // When:
      waitForRemoteServerToChangeStatus(
          host0, HighAvailabilityTestUtil::remoteServerIsUp, KEYSTORE_PROPS, LISTENER0, true,
          Optional.of(USER1), HOST_ALIAS_RESOLVER);
      waitForRemoteServerToChangeStatus(
          host1, HighAvailabilityTestUtil::remoteServerIsUp, KEYSTORE_PROPS, LISTENER0, true,
          Optional.of(USER1), HOST_ALIAS_RESOLVER);
      ClusterStatusResponse response = sendClusterStatusRequest(KEYSTORE_PROPS,
          LISTENER0, true, Optional.of(USER1), HOST_ALIAS_RESOLVER);

      // Then:
      assertThat(response.getClusterStatus().get(host0).getHostAlive(), is(true));
      assertThat(response.getClusterStatus().get(host1).getHostAlive(), is(true));
    }
  }

  private static void allowAccess(final KsqlAuthorizationProvider ap,
      final BasicCredentials user, final String method, final String path) {
    doNothing().when(ap)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private static class PrincipalMatcher implements ArgumentMatcher<Principal> {

    private final BasicCredentials user;

    PrincipalMatcher(final BasicCredentials user) {
      this.user = user;
    }

    @Override
    public boolean matches(final Principal principal) {
      return this.user.username().equals(principal.getName());
    }
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }

  private static class LocalhostResolver implements HostAliasResolver {

    @Override
    public String resolve(String host) {
      return "localhost";
    }
  }
}
