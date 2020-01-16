/*
 * Copyright 2019 Confluent Inc.
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
    import java.util.Map;
    import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LagReportingRequest {

  private HostInfoEntity hostInfoEntity;
  private Map<String, Map<Integer, LagInfoEntity>> storeToPartitionToLagMap;
  private long lastLagUpdateMs;

  @JsonCreator
  public LagReportingRequest(
      @JsonProperty("hostInfoEntity") final HostInfoEntity hostInfoEntity,
      @JsonProperty("storeToPartitionToLagMap") final Map<String, Map<Integer, LagInfoEntity>>
          storeToPartitionToLagMap,
      @JsonProperty("lastLagUpdateMs") final long lastLagUpdateMs
  ) {
    this.hostInfoEntity = Objects.requireNonNull(hostInfoEntity, "hostInfoEntity");
    this.storeToPartitionToLagMap = storeToPartitionToLagMap;
    this.lastLagUpdateMs = lastLagUpdateMs;
  }

  public HostInfoEntity getHostInfoEntity() {
    return hostInfoEntity;
  }

  public Map<String, Map<Integer, LagInfoEntity>> getStoreToPartitionToLagMap() {
    return storeToPartitionToLagMap;
  }

  public long getLastLagUpdateMs() {
    return lastLagUpdateMs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LagReportingRequest that = (LagReportingRequest) o;
    return Objects.equals(hostInfoEntity, that.hostInfoEntity)
        && Objects.equals(storeToPartitionToLagMap, that.storeToPartitionToLagMap)
        && lastLagUpdateMs == that.lastLagUpdateMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostInfoEntity, storeToPartitionToLagMap, lastLagUpdateMs);
  }

  @Override
  public String toString() {
    return hostInfoEntity + "," + storeToPartitionToLagMap + "," + lastLagUpdateMs;
  }
}