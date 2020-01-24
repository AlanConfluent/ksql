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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Map;
import java.util.Objects;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterLagsResponse {
  private final Map<HostInfoEntity, Map<QueryStateStoreId, Map<Integer, LagInfoEntity>>> lags;

  @JsonCreator
  public ClusterLagsResponse(
      @JsonProperty("lags")
      final Map<HostInfoEntity, Map<QueryStateStoreId, Map<Integer, LagInfoEntity>>> lags
  ) {
    this.lags = lags;
  }

  public Map<HostInfoEntity, Map<QueryStateStoreId, Map<Integer, LagInfoEntity>>> getLags() {
    return lags;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ClusterLagsResponse that = (ClusterLagsResponse) o;
    return Objects.equals(lags, that.lags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lags);
  }
}
