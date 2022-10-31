/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.balancer;

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Balancer 参数
 * */
@InterfaceAudience.Private
final class BalancerParameters {
  /**
   * Balancer 策略
   * 集群内部：DataNode 级别 (默认)
   * HDFS Federation ：BlockPool级别
   */
  private final BalancingPolicy policy;
  /**
   * 平衡使用率差,默认 10%
   * */
  private final double threshold;
  /**
   * 最大连续空闲迭代次数，默认 5
   * */
  private final int maxIdleIteration;
  /** Exclude the nodes in this set.
   *  不允许参与  Balancer 的 DataNode 列表
   * */
  private final Set<String> excludedNodes;
  /** If empty, include any node; otherwise, include only these nodes.
   *  指定参与 Balancer 的  DataNode 列表
   * */
  private final Set<String> includedNodes;
  /**
   * If empty, any node can be a source; otherwise, use only these nodes as
   * source nodes.
   * 指定 DataNode 列表作为 源节点
   */
  private final Set<String> sourceNodes;
  /**
   * A set of block pools to run the balancer on.
   * Federation 下参与 Balancer 的 blockpools 列表
   */
  private final Set<String> blockpools;
  /**
   * Whether to run the balancer during upgrade.
   * 是否允许在正在进行的HDFS升级期间进行 Balancer 操作，默认 false
   */
  private final boolean runDuringUpgrade;

  static final BalancerParameters DEFAULT = new BalancerParameters();

  private BalancerParameters() {
    this(new Builder());
  }

  private BalancerParameters(Builder builder) {
    this.policy = builder.policy;
    this.threshold = builder.threshold;
    this.maxIdleIteration = builder.maxIdleIteration;
    this.excludedNodes = builder.excludedNodes;
    this.includedNodes = builder.includedNodes;
    this.sourceNodes = builder.sourceNodes;
    this.blockpools = builder.blockpools;
    this.runDuringUpgrade = builder.runDuringUpgrade;
  }

  BalancingPolicy getBalancingPolicy() {
    return this.policy;
  }

  double getThreshold() {
    return this.threshold;
  }

  int getMaxIdleIteration() {
    return this.maxIdleIteration;
  }

  Set<String> getExcludedNodes() {
    return this.excludedNodes;
  }

  Set<String> getIncludedNodes() {
    return this.includedNodes;
  }

  Set<String> getSourceNodes() {
    return this.sourceNodes;
  }

  Set<String> getBlockPools() {
    return this.blockpools;
  }

  boolean getRunDuringUpgrade() {
    return this.runDuringUpgrade;
  }

  @Override
  public String toString() {
    return String.format("%s.%s [%s," + " threshold = %s,"
        + " max idle iteration = %s," + " #excluded nodes = %s,"
        + " #included nodes = %s," + " #source nodes = %s,"
        + " #blockpools = %s," + " run during upgrade = %s]",
        Balancer.class.getSimpleName(), getClass().getSimpleName(), policy,
        threshold, maxIdleIteration, excludedNodes.size(),
        includedNodes.size(), sourceNodes.size(), blockpools.size(),
        runDuringUpgrade);
  }

  static class Builder {
    // Defaults
    private BalancingPolicy policy = BalancingPolicy.Node.INSTANCE;
    private double threshold = 10.0;
    private int maxIdleIteration =
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS;
    private Set<String> excludedNodes = Collections.<String> emptySet();
    private Set<String> includedNodes = Collections.<String> emptySet();
    private Set<String> sourceNodes = Collections.<String> emptySet();
    private Set<String> blockpools = Collections.<String> emptySet();
    private boolean runDuringUpgrade = false;

    Builder() {
    }

    Builder setBalancingPolicy(BalancingPolicy p) {
      this.policy = p;
      return this;
    }

    Builder setThreshold(double t) {
      this.threshold = t;
      return this;
    }

    Builder setMaxIdleIteration(int m) {
      this.maxIdleIteration = m;
      return this;
    }

    Builder setExcludedNodes(Set<String> nodes) {
      this.excludedNodes = nodes;
      return this;
    }

    Builder setIncludedNodes(Set<String> nodes) {
      this.includedNodes = nodes;
      return this;
    }

    Builder setSourceNodes(Set<String> nodes) {
      this.sourceNodes = nodes;
      return this;
    }

    Builder setBlockpools(Set<String> pools) {
      this.blockpools = pools;
      return this;
    }

    Builder setRunDuringUpgrade(boolean run) {
      this.runDuringUpgrade = run;
      return this;
    }

    BalancerParameters build() {
      return new BalancerParameters(this);
    }
  }
}