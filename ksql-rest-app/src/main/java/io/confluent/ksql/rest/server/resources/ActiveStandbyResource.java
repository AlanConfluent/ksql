/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ActiveStandbyResponse;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

/**
 * Endpoint for registering heartbeats received from remote servers. The heartbeats are used
 @AlanConfluent
 AlanConfluent yesterday
 nit: Wrong comment.

 @AlanConfluent	Reply…
  * to determine the status of the remote servers, i.e. whether they are alive or dead.
 */

@Path("/activeStandby")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class ActiveStandbyResource {

  private final KsqlEngine engine;
  private HostInfo localHostInfo;

  public ActiveStandbyResource(final KsqlEngine engine) {
    this.engine = engine;
  }

//  public void setLocalHostInfo(final String applicationServerId) {
//    this.localHostInfo = ServerUtil.parseHostInfo(applicationServerId);
//  }

  @GET
  public Response getActiveStandbyInformation() {
    return Response.ok(getResponse()).build();
  }

  @VisibleForTesting
  ActiveStandbyResponse getResponse() {
    final List<PersistentQueryMetadata> currentQueries = engine.getPersistentQueries();
    if (currentQueries.isEmpty()) {
      // empty response
      return new ActiveStandbyResponse(Collections.emptyMap());
    }

    final Map<String, ActiveStandbyEntity> perQueryMap = new HashMap<>();
    for (PersistentQueryMetadata persistentMetadata: currentQueries) {
      for (StreamsMetadata streamsMetadata : ((QueryMetadata)persistentMetadata).getAllMetadata()) {
        if (streamsMetadata == null || !streamsMetadata.hostInfo().equals(localHostInfo)) {
          continue;
        }
        final ActiveStandbyEntity entity = new ActiveStandbyEntity(
            streamsMetadata.stateStoreNames(),
            streamsMetadata.topicPartitions()
                .stream()
                .map(TopicPartition::toString)
                .collect(Collectors.toSet()),
            streamsMetadata.standbyStateStoreNames(),
            streamsMetadata.standbyTopicPartitions()
                .stream()
                .map(TopicPartition::toString)
                .collect(Collectors.toSet()));
        perQueryMap.put(((QueryMetadata)persistentMetadata).getQueryApplicationId(), entity);
      }

    }
    return new ActiveStandbyResponse(perQueryMap);
  }
}