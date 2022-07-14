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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import com.google.common.annotations.VisibleForTesting;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine, 
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 *
 * 1 replica. 第一个副本如果写请求方所在机器是其中一个datanode,首先选择存放在本机 datanode,否则随机在集群中选择一个datanode.
 * 2 replica. 第二个副本存放在与第一个副本不同的机架 datanode.
 * 3 replica. 第三个副本存放在第二个副本所在的机架,切实不同的节点. （如果这个机器架没节点可以存放呢？怎么处理？ TODO）
 *
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on "
          + BlockPlacementPolicy.class.getName() + " and "
          + NetworkTopology.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  private static final ThreadLocal<HashMap<NodeNotChosenReason, Integer>>
      CHOOSE_RANDOM_REASONS = ThreadLocal
      .withInitial(() -> new HashMap<NodeNotChosenReason, Integer>());

  private enum NodeNotChosenReason {
    NOT_IN_SERVICE("the node isn't in service"),
    NODE_STALE("the node is stale"),
    NODE_TOO_BUSY("the node is too busy"),
    TOO_MANY_NODES_ON_RACK("the rack has too many chosen nodes"),
    NOT_ENOUGH_STORAGE_SPACE("no enough storage space to place the block");

    private final String text;

    NodeNotChosenReason(final String logText) {
      text = logText;
    }

    private String getText() {
      return text;
    }
  }

  protected boolean considerLoad; 
  protected double considerLoadFactor;
  private boolean preferLocalNode;
  protected NetworkTopology clusterMap;
  protected Host2NodesMap host2datanodeMap;
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  
  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  protected BlockPlacementPolicyDefault() {
  }
    
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_DEFAULT);
    this.considerLoadFactor = conf.getDouble(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR_DEFAULT);
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS) * 1000;
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.preferLocalNode = conf.getBoolean(
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_KEY,
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_DEFAULT);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> flags) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags);
  }

  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // Favored nodes not specified, fall back to regular block placement.
        //未指定"优先分配的节点" 则执行该常规逻辑
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
            excludedNodes, blocksize, storagePolicy, flags);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<>(excludedNodes);
      final List<StorageType> requiredStorageTypes = storagePolicy
          .chooseStorageTypes((short)numOfReplicas);
      final EnumMap<StorageType, Integer> storageTypes =
          getRequiredStorageTypes(requiredStorageTypes);

      // Choose favored nodes
      List<DatanodeStorageInfo> results = new ArrayList<>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();

      int maxNodesAndReplicas[] = getMaxNodesPerRack(0, numOfReplicas);
      numOfReplicas = maxNodesAndReplicas[0];
      int maxNodesPerRack = maxNodesAndReplicas[1];

      chooseFavouredNodes(src, numOfReplicas, favoredNodes,
          favoriteAndExcludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes, based on block
        // placement policy (HDFS-9393).
        numOfReplicas -= results.size();
        for (DatanodeStorageInfo storage : results) {
          // add localMachine and related nodes to favoriteAndExcludedNodes
          addToExcludedNodes(storage.getDatanodeDescriptor(),
              favoriteAndExcludedNodes);
        }
        DatanodeStorageInfo[] remainingTargets =
            chooseTarget(src, numOfReplicas, writer,
                new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
                favoriteAndExcludedNodes, blocksize, storagePolicy, flags);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose with favored nodes (=" + favoredNodes
            + "), disregard favored nodes hint and retry.", nr);
      }
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
          excludedNodes, blocksize, storagePolicy, flags);
    }
  }

  protected void chooseFavouredNodes(String src, int numOfReplicas,
      List<DatanodeDescriptor> favoredNodes,
      Set<Node> favoriteAndExcludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas;
        i++) {
      DatanodeDescriptor favoredNode = favoredNodes.get(i);
      // Choose a single node which is local to favoredNode.
      // 'results' is updated within chooseLocalNode
      final DatanodeStorageInfo target =
          chooseLocalStorage(favoredNode, favoriteAndExcludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes, false);
      if (target == null) {
        LOG.warn("Could not find a target for file " + src
            + " with favored node " + favoredNode);
        continue;
      }
      favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
    }
  }

  /** This is the implementation.
   *  具体的实现
   * */
  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenStorage,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> addBlockFlags) {
     //如果目标的副本个数和当前集群节点个数为 0 则返回一个空数组
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }

    //如果 "excludedNodes" 为空则为其创建一个集合存放 excluded 节点
    if (excludedNodes == null) {
      excludedNodes = new HashSet<>();
    }
    //返回二维数组 ：{允许分配的总节点个数，每个机架允许分配的最大节点个数}
    int[] result = getMaxNodesPerRack(chosenStorage.size(), numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];

    //将已经分配的节点加入到 excludedNodes 名单中，避免再次被分配到
    for (DatanodeStorageInfo storage : chosenStorage) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }

    List<DatanodeStorageInfo> results = null;
    Node localNode = null;
    //是否避免将数据写入过时的节点
    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    //是否避免数据本地化写入
    boolean avoidLocalNode = (addBlockFlags != null
        && addBlockFlags.contains(AddBlockFlag.NO_LOCAL_WRITE)
        && writer != null
        && !excludedNodes.contains(writer));

    // Attempt to exclude local node if the client suggests so. If no enough
    // nodes can be obtained, it falls back to the default block placement
    // policy

    if (avoidLocalNode) { //不期望数据写入local 节点逻辑
      results = new ArrayList<>(chosenStorage);
      Set<Node> excludedNodeCopy = new HashSet<>(excludedNodes);
      excludedNodeCopy.add(writer);
      localNode = chooseTarget(numOfReplicas, writer,
          excludedNodeCopy, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storagePolicy,
          EnumSet.noneOf(StorageType.class), results.isEmpty()); //如果还没有分配任何节点则，当前行为为 newBlock
      if (results.size() < numOfReplicas) {
        // not enough nodes; discard results and fall back  节点不足返回退出
        results = null;
      }
    }

    //走默认置放策略
    if (results == null) {
      results = new ArrayList<>(chosenStorage);
      // localNode 为第一个选取的节点
      localNode = chooseTarget(numOfReplicas, writer, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storagePolicy, EnumSet.noneOf(StorageType.class), results.isEmpty());
    }

    // 如果不返回初始选中的目标节点,则进行移除
    if (!returnChosenNodes) {
      results.removeAll(chosenStorage);
    }


    // sorting nodes to form a pipeline  根据网络拓关系返回一个  pipeline 结果  （与 Client 的远近关系）
    return getPipeline(
        (writer != null && writer instanceof DatanodeDescriptor) ? writer
            : localNode,
        results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  /**
   * Calculate the maximum number of replicas to allocate per rack. It also
   * limits the total number of replicas to the total number of nodes in the
   * cluster. Caller should adjust the replica count to the return value.
   *  计算每个机架节点允许分配的副本个数
   *  最终返回允许分配的总副本个数
   * @param numOfChosen The number of already chosen nodes. 已经分配的副本节点个数
   * @param numOfReplicas The number of additional nodes to allocate. 期望分配的副本节点个数
   * @return integer array. Index 0: The number of nodes allowed to allocate
   *         in addition to already chosen nodes.
   *         Index 1: The maximum allowed number of nodes per rack. This
   *         is independent of the number of chosen nodes, as it is calculated
   *         using the target number of replicas.
   *  返回一个数组：{允许分配的总节点个数，每个机架允许分配的最大节点个数}
   *  TODO 解读不透测
   */
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves(); //集群存活节点个数
    int totalNumOfReplicas = numOfChosen + numOfReplicas; //需求的总资源数
    if (totalNumOfReplicas > clusterSize) { //如果资源总需求量超过节点个数
      // numOfReplicas = numOfReplicas - （totalNumOfReplicas-clusterSize）
      // numOfReplicas 等于在该集群中允许分配的副本（不在同一节点）
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      //集群总节点数此时为能满足需求的最大副本个数
      totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfRacks(); //总机架个数
    // 如果只有一个机架时，且总副本资源小于等于 1 时直接返回
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      // return {1,1}
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }

    //计算每个 rack 允许分配的副本个数
    int maxNodesPerRack = (totalNumOfReplicas-1)/numOfRacks + 2;
    // At this point, there are more than one racks and more than one replicas
    // to store. Avoid all replicas being in the same rack.
    //
    // maxNodesPerRack has the following properties at this stage.
    //   1) maxNodesPerRack >= 2
    //   2) (maxNodesPerRack-1) * numOfRacks > totalNumOfReplicas
    //          when numOfRacks > 1
    //
    // Thus, the following adjustment will still result in a value that forces
    // multi-rack allocation and gives enough number of total nodes.
    if (maxNodesPerRack == totalNumOfReplicas) {
      maxNodesPerRack--;
    }

    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  private EnumMap<StorageType, Integer> getRequiredStorageTypes(
      List<StorageType> types) {
    EnumMap<StorageType, Integer> map = new EnumMap<>(StorageType.class);
    for (StorageType type : types) {
      if (!map.containsKey(type)) {
        map.put(type, 1);
      } else {
        int num = map.get(type);
        map.put(type, num + 1);
      }
    }
    return map;
  }

  /**
   * choose <i>numOfReplicas</i> from all data nodes
   * @param numOfReplicas additional number of replicas wanted
   *                       需要的副本个数
   * @param writer the writer's machine, could be a non-DatanodeDescriptor node
   *               写入数据的数据源节点
   * @param excludedNodes datanodes that should not be considered as targets
   *                       不在考虑范围之内的节点集合
   * @param blocksize size of the data to be written
   *                   写入的数据块大小
   * @param maxNodesPerRack max nodes allowed per rack
   *                    同一个 rack 最多允许分配的副本个数
   * @param results the target nodes already chosen
   *                 当前已经选好的副本目标节点集合
   * @param avoidStaleNodes avoid stale nodes in replica choosing
   *                  避免选择过期的节点 TODO  stale node？怎么定义的
   * @param storagePolicy 副本置放策略
   * @param newBlock   如果 results 为空，则为 new Block
   * @return local node of writer (not chosen node)
   */
  private Node chooseTarget(int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            final BlockStoragePolicy storagePolicy,  //副本置放策略
                            final EnumSet<StorageType> unavailableStorages,
                            final boolean newBlock) { //如果 results 为空，则为 new Block
    //如果副本需求为 0 且集群存活的节点个数为 0
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      // 如果writer请求者在其中一个datanode上则返回此节点,否则直接返回null
      return (writer instanceof DatanodeDescriptor) ? writer : null;
    }
    final int numOfResults = results.size();
    final int totalReplicasExpected = numOfReplicas + numOfResults;
    //如果 results 中已经有选定的结果，则直接从 results 中获取第一个节点作为 writer
    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    //复制排除的节点集合
    final Set<Node> oldExcludedNodes = new HashSet<>(excludedNodes);

    // choose storage types; use fallbacks for unavailable storages
    //TODO 数据类型存储策略
    // 根据存储策略获取副本需要满足的存储类型列表,如果有不可用的存储类型,会采用fallback的类型
    // requiredStorageTypes 期望存储的数据类型，unavailableStorages 不可用的存储类型
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) totalReplicasExpected,
            DatanodeStorageInfo.toStorageTypes(results),
            unavailableStorages, newBlock);

    //将存储类型列表进行计数统计,并存于map中 todo
    final EnumMap<StorageType, Integer> storageTypes =
        getRequiredStorageTypes(requiredStorageTypes);
    if (LOG.isTraceEnabled()) {
      LOG.trace("storageTypes=" + storageTypes);
    }

    try {
      //如果 requiredStorageTypes 大小为 0 则抛出异常 ， requiredStorageTypes 能实际满足的数据类型副本个数
      if ((numOfReplicas = requiredStorageTypes.size()) == 0) {
        throw new NotEnoughReplicasException(
            "All required storage types are unavailable: "
            + " unavailableStorages=" + unavailableStorages
            + ", storagePolicy=" + storagePolicy);
      }
      //按照策略获取副本
      writer = chooseTargetInOrder(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, newBlock, storageTypes);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach "
          + totalReplicasExpected
          + " (unavailableStorages=" + unavailableStorages
          + ", storagePolicy=" + storagePolicy
          + ", newBlock=" + newBlock + ")";

      if (LOG.isTraceEnabled()) {
        LOG.trace(message, e);
      } else {
        //打印异常信息，没有找到 storagePolicy 的副本
        LOG.warn(message + " " + e.getMessage());
      }

      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the 
        // result list in the successful calls to choose*() above.
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock);
      }

      boolean retry = false;
      // simply add all the remaining types into unavailableStorages and give
      // another try. No best effort is guaranteed here.
      for (StorageType type : storageTypes.keySet()) {
        if (!unavailableStorages.contains(type)) {
          unavailableStorages.add(type);
          retry = true;
        }
      }
      if (retry) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
              oldExcludedNodes);
        }
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock);
      }
    }
    return writer;
  }

  protected Node chooseTargetInOrder(int numOfReplicas, 
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    final int numOfResults = results.size();
    // 如果已选择的目标节点数量为0,则表示3副本一个都还没开始选,首先从选本地节点开始
    if (numOfResults == 0) {
      writer = chooseLocalStorage(writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
          .getDatanodeDescriptor();
      // 如果此时目标需求完成的副本数为降为0,代表选择目标完成,返回第一个节点writer
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    //去除 results 列表中第一个节点
    final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
    if (numOfResults <= 1) {
      // 前面的过程已经完成首个本地节点的选择,此时进行不同机架的节点选择,如果其它机架没有足够的节点，回退策略会从同机架节点随机选择一个节点
      chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
      // 如果此时目标需求完成的副本数为降为0,代表选择目标完成,返回第一个节点writer
      if (--numOfReplicas == 0) {
        return writer;
      }
    }

    // 如果经过前面的处理,节点选择数在2个以内,需要选取第3个副本
    if (numOfResults <= 2) {
      final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
      //如果 dn0 与 dn1 同机架，则 dn2 必须选择不同机架
      if (clusterMap.isOnSameRack(dn0, dn1)) {
        // 则选择1个不同于dn0,dn1所在机架的副本位置
        chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else if (newBlock){
        // 如果是新的block块,，且 dn0  与 dn1 不同机架，则选取1个与 dn1 所在同机房的节点位置
        chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      } else {
        // 否则选取于 writer 同机架的位置
        chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes);
      }

      // 如果此时目标需求完成的副本数为降为0,代表选择目标完成,返回第一个节点writer
      if (--numOfReplicas == 0) {
        return writer;
      }
    }
    // 如果副本数已经超过2个,说明设置的block的时候,已经设置超过3副本的数量
    // 则剩余位置在集群中随机选择放置节点
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    //如果 localMachine 为空则降级随机选择一个节点
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor
        && clusterMap.contains(localMachine)) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine) // was not in the excluded list
             // 判断是否为可以保存数据块的节点
          && isGoodDatanode(localDatanode, maxNodesPerRack, false,
              results, avoidStaleNodes)) {
        // 遍历本地节点可用的存储目录
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          //判定节点是否足够的 StorageType 资源余量分配给当前块
          DatanodeStorageInfo localStorage = chooseStorage4Block(
              localDatanode, blocksize, results, entry.getKey());
          if (localStorage != null) {
            // add node and related nodes to excludedNode 分配成功，同时将入 excludedNodes 列表
            addToExcludedNodes(localDatanode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            return localStorage;
          }
        }
      } 
    }
    return null;
  }

  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    //选择当前节点为 写入目标节点
    DatanodeStorageInfo localStorage = chooseLocalStorage(localMachine,
        excludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (localStorage != null) {
      return localStorage;
    }

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack 如果还本地节点选择失败，则选择 local 节点的同机架节点
    return chooseLocalRack(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }
  
  /**
   * Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
   * for next replica choosing. In sub class, we can add more nodes within
   * the same failure domain of localMachine
   * @return number of new excluded nodes
   */
  protected int addToExcludedNodes(DatanodeDescriptor localMachine,
      Set<Node> excludedNodes) {
    return excludedNodes.add(localMachine) ? 1 : 0;
  }

  /**
   * Choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node 
   * in the cluster.
   * @return the chosen node
   */
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
                                                Set<Node> excludedNodes,
                                                long blocksize,
                                                int maxNodesPerRack,
                                                List<DatanodeStorageInfo> results,
                                                boolean avoidStaleNodes,
                                                EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();
      
    try {
      // choose one from the local rack  从同机架中随机选择一个节点
      return chooseRandom(localRack, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to choose from local rack (location = " + localRack
                + "), retry with the rack of the next replica (location = "
                + nextNode.getNetworkLocation() + ")", e);
          }
          return chooseFromNextRack(nextNode, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from local rack (location = " + localRack
            + "); the second replica is not found, retry choosing randomly", e);
      }
      //the second replica is not found, randomly choose one from the network
      //如果同机架节点选择失败，则随机选择一个节点
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  private DatanodeStorageInfo chooseFromNextRack(Node next,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    final String nextRack = next.getNetworkLocation();
    try {
      return chooseRandom(nextRack, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch(NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from the next rack (location = " + nextRack
            + "), retry choosing randomly", e);
      }
      //otherwise randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /** 
   * Choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * If not enough nodes are available, choose the remaining ones
   * from the local rack
   */
  protected void chooseRemoteRack(int numOfReplicas,
                                DatanodeDescriptor localMachine,
                                Set<Node> excludedNodes,
                                long blocksize,
                                int maxReplicasPerRack,
                                List<DatanodeStorageInfo> results,
                                boolean avoidStaleNodes,
                                EnumMap<StorageType, Integer> storageTypes)
                                    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
            + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      //TODO 策略回退
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Randomly choose one target from the given <i>scope</i>.
   * @return the chosen storage, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(String scope,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
          throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes);
  }

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * @return the first chosen node, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(int numOfReplicas,
                            String scope,
                            Set<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeStorageInfo> results,
                            boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    CHOOSE_RANDOM_REASONS.get().clear();
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while (numOfReplicas > 0) {
      // the storage type that current node has
      StorageType includeType = null;
      DatanodeDescriptor chosenNode = null;
      if (clusterMap instanceof DFSNetworkTopology) {
        for (StorageType type : storageTypes.keySet()) {
          chosenNode = chooseDataNode(scope, excludedNodes, type);

          if (chosenNode != null) {
            includeType = type;
            break;
          }
        }
      } else {
        chosenNode = chooseDataNode(scope, excludedNodes);
      }

      if (chosenNode == null) {
        break;
      }
      Preconditions.checkState(excludedNodes.add(chosenNode), "chosenNode "
          + chosenNode + " is already in excludedNodes " + excludedNodes);
      if (LOG.isDebugEnabled() && builder != null) {
        builder.append("\nNode ").append(NodeBase.getPath(chosenNode))
            .append(" [");
      }
      DatanodeStorageInfo storage = null;
      if (isGoodDatanode(chosenNode, maxNodesPerRack, considerLoad,
          results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext();) {
          Map.Entry<StorageType, Integer> entry = iter.next();

          // If there is one storage type the node has already contained,
          // then no need to loop through other storage type.
          if (includeType != null && entry.getKey() != includeType) {
            continue;
          }
          storage = chooseStorage4Block(
              chosenNode, blocksize, results, entry.getKey());
          if (storage != null) {
            numOfReplicas--;
            if (firstChosen == null) {
              firstChosen = storage;
            }
            // add node (subclasses may also add related nodes) to excludedNode
            addToExcludedNodes(chosenNode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            break;
          }
        }

        if (LOG.isDebugEnabled() && builder != null) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (storage == null);
      }
    }
    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled() && builder != null) {
        detail = builder.toString();
        if (badTarget) {
          builder.setLength(0);
        } else {
          if (detail.length() > 1) {
            // only log if there's more than "[", which is always appended at
            // the beginning of this method.
            LOG.debug(detail);
          }
          detail = "";
        }
      }
      final HashMap<NodeNotChosenReason, Integer> reasonMap =
          CHOOSE_RANDOM_REASONS.get();
      if (!reasonMap.isEmpty()) {
        LOG.info("Not enough replicas was chosen. Reason:{}", reasonMap);
      }
      throw new NotEnoughReplicasException(detail);
    }
    
    return firstChosen;
  }

  /**
   * Choose a datanode from the given <i>scope</i>.
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNodes) {
    return (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNodes);
  }

  /**
   * Choose a datanode from the given <i>scope</i> with specified
   * storage type.
   * @return the chosen node, if there is any.
   */
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    return (DatanodeDescriptor) ((DFSNetworkTopology) clusterMap)
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNodes, type);
  }

  /**
   * Choose a good storage of given storage type from datanode, and add it to
   * the result list.
   *  从节点选择一个合适的 storage 分配，并加入到 result 列表中
   * @param dnd datanode descriptor
   * @param blockSize requested block size
   * @param results the result storages
   * @param storageType requested storage type
   * @return the chosen datanode storage
   */
  DatanodeStorageInfo chooseStorage4Block(DatanodeDescriptor dnd,
      long blockSize,
      List<DatanodeStorageInfo> results,
      StorageType storageType) {
    DatanodeStorageInfo storage =
        dnd.chooseStorage4Block(storageType, blockSize);
    if (storage != null) {
      results.add(storage);
    } else {
      //如果没有分配到，打印日志没有足够的空间分配给 block
      logNodeIsNotChosen(dnd, NodeNotChosenReason.NOT_ENOUGH_STORAGE_SPACE,
          " for storage type " + storageType);
    }
    return storage;
  }

  private static void logNodeIsNotChosen(DatanodeDescriptor node,
      NodeNotChosenReason reason) {
    logNodeIsNotChosen(node, reason, null);
  }

  private static void logNodeIsNotChosen(DatanodeDescriptor node,
      NodeNotChosenReason reason, String reasonDetails) {
    assert reason != null;
    if (LOG.isDebugEnabled()) {
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append("\n  Datanode ").append(node)
          .append(" is not chosen since ").append(reason.getText());
      if (reasonDetails != null) {
        debugLoggingBuilder.get().append(" ").append(reasonDetails);
      }
      debugLoggingBuilder.get().append(".");
    }
    // always populate reason map to log high level reasons.
    final HashMap<NodeNotChosenReason, Integer> reasonMap =
        CHOOSE_RANDOM_REASONS.get();
    Integer base = reasonMap.get(reason);
    if (base == null) {
      base = 0;
    }
    reasonMap.put(reason, base + 1);
  }

  /**
   * Determine if a datanode is good for placing block.
   * 判断是否为可以保存数据块的节点
   * @param node The target datanode
   * @param maxTargetPerRack Maximum number of targets per rack. The value of
   *                       this parameter depends on the number of racks in
   *                       the cluster and total number of replicas for a block
   * @param considerLoad whether or not to consider load of the target node
   * @param results A list containing currently chosen nodes. Used to check if
   *                too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes Whether or not to avoid choosing stale nodes
   * @return Reture true if the datanode is good candidate, otherwise false
   */
  boolean isGoodDatanode(DatanodeDescriptor node,
                         int maxTargetPerRack, boolean considerLoad,
                         List<DatanodeStorageInfo> results,
                         boolean avoidStaleNodes) {
    // check if the node is (being) decommissioned
    if (!node.isInService()) {
      logNodeIsNotChosen(node, NodeNotChosenReason.NOT_IN_SERVICE);
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(node, NodeNotChosenReason.NODE_STALE);
        return false;
      }
    }

    // check the communication traffic of the target machine
    if (considerLoad) {
      final double maxLoad = considerLoadFactor *
          stats.getInServiceXceiverAverage();
      final int nodeLoad = node.getXceiverCount();
      if (nodeLoad > maxLoad) {
        logNodeIsNotChosen(node, NodeNotChosenReason.NODE_TOO_BUSY,
            "(load: " + nodeLoad + " > " + maxLoad + ")");
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(DatanodeStorageInfo resultStorage : results) {
      if (rackname.equals(
          resultStorage.getDatanodeDescriptor().getNetworkLocation())) {
        counter++;
      }
    }
    if (counter > maxTargetPerRack) {
      logNodeIsNotChosen(node, NodeNotChosenReason.TOO_MANY_NODES_ON_RACK);
      return false;
    }

    return true;
  }

  /**
   * Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   * TSP旅行商问题：
   * - 从writer所在节点开始,总是寻找相对路径最短的目标节点,最终形成pipeline
   * - 每两个节点距离是最短的，整个 pipeline 距离总是最近的
   */
  private DatanodeStorageInfo[] getPipeline(Node writer,
      DatanodeStorageInfo[] storages) {
    if (storages.length == 0) {
      return storages;
    }

    synchronized(clusterMap) {
      int index=0;
      // 首先如果writer请求方本身不在一个datanode上,则默认选取第一个 datanode 作为起始节点
      //  TODO 跨Az 的情况就得注意，不能随意这样获取
      if (writer == null || !clusterMap.contains(writer)) {
        writer = storages[0].getDatanodeDescriptor();
      }
      for(; index < storages.length; index++) {
        // 获取当前index下标所属的Storage为最近距离的目标storage
        DatanodeStorageInfo shortestStorage = storages[index];
        //计算 index 的 datanode 与 writer 的距离  ， TODO 跨 Az 的场景该怎么计算距离
        int shortestDistance = clusterMap.getDistance(writer,
            shortestStorage.getDatanodeDescriptor());
        int shortestIndex = index;

        //遍历计算对比后面的 storage，获取距离近的 storage
        for(int i = index + 1; i < storages.length; i++) {
          int currentDistance = clusterMap.getDistance(writer,
              storages[i].getDatanodeDescriptor());
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestStorage = storages[i];
            shortestIndex = i;
          }
        }
        //找到距离最近的 storage  代替 index
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          storages[shortestIndex] = storages[index];
          storages[index] = shortestStorage;
        }
        //执行一轮完毕后，再更新 writer 并进行下一次迭代
        writer = shortestStorage.getDatanodeDescriptor();
      }
    }
    return storages;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // only one rack
      return new BlockPlacementStatusDefault(1, 1, 1);
    }
    int minRacks = 2;
    minRacks = Math.min(minRacks, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), minRacks,
        clusterMap.getNumOfRacks());
  }
  /**
   * Decide whether deleting the specified replica of the block still makes
   * the block conform to the configured block placement policy.
   * @param moreThanOne The replica locations of this block that are present
   *                    on more than one unique racks.
   * @param exactlyOne Replica locations of this block that  are present
   *                    on exactly one unique racks.
   * @param excessTypes The excess {@link StorageType}s according to the
   *                    {@link BlockStoragePolicy}.
   *
   * @return the replica that is the best candidate for deletion
   */
  @VisibleForTesting
  public DatanodeStorageInfo chooseReplicaToDelete(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      final List<StorageType> excessTypes,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    long oldestHeartbeat =
      monotonicNow() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for(DatanodeStorageInfo storage : pickupReplicaSet(moreThanOne,
        exactlyOne, rackMap)) {
      if (!excessTypes.contains(storage.getStorageType())) {
        continue;
      }

      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = storage.getRemaining();
      long lastHeartbeat = node.getLastUpdateMonotonic();
      if (lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    final DatanodeStorageInfo storage;
    if (oldestHeartbeatStorage != null) {
      storage = oldestHeartbeatStorage;
    } else if (minSpaceStorage != null) {
      storage = minSpaceStorage;
    } else {
      return null;
    }
    excessTypes.remove(storage.getStorageType());
    return storage;
  }

  @Override
  public List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates,
      int expectedNumOfReplicas,
      List<StorageType> excessTypes,
      DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {

    List<DatanodeStorageInfo> excessReplicas = new ArrayList<>();

    final Map<String, List<DatanodeStorageInfo>> rackMap = new HashMap<>();

    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();

    // split candidate nodes for deletion into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    splitNodesWithRack(availableReplicas, delCandidates, rackMap, moreThanOne,
        exactlyOne);

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    final DatanodeStorageInfo delNodeHintStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(delCandidates, delNodeHint);
    final DatanodeStorageInfo addedNodeStorage =
        DatanodeStorageInfo.getDatanodeStorageInfo(delCandidates, addedNode);

    while (delCandidates.size() - expectedNumOfReplicas > excessReplicas.size()) {
      final DatanodeStorageInfo cur;
      if (firstOne && useDelHint(delNodeHintStorage, addedNodeStorage,
          moreThanOne, exactlyOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        cur = chooseReplicaToDelete(moreThanOne, exactlyOne,
            excessTypes, rackMap);
      }
      firstOne = false;
      if (cur == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No excess replica can be found. excessTypes: {}." +
              " moreThanOne: {}. exactlyOne: {}.", excessTypes,
              moreThanOne, exactlyOne);
        }
        break;
      }

      // adjust rackmap, moreThanOne, and exactlyOne
      adjustSetsWithChosenReplica(rackMap, moreThanOne, exactlyOne, cur);
      excessReplicas.add(cur);
    }
    return excessReplicas;
  }

  /** Check if we can use delHint. */
  @VisibleForTesting
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    if (delHint == null) {
      return false; // no delHint
    } else if (!excessTypes.contains(delHint.getStorageType())) {
      return false; // delHint storage type is not an excess type
    } else {
      // check if removing delHint reduces the number of racks
      return notReduceNumOfGroups(moreThanOne, delHint, added);
    }
  }

  // Check if moving from source to target will reduce the number of
  // groups. The groups could be based on racks or upgrade domains.
  <T> boolean notReduceNumOfGroups(List<T> moreThanOne, T source, T target) {
    if (moreThanOne.contains(source)) {
      return true; // source and some other nodes are under the same group.
    } else if (target != null && !moreThanOne.contains(target)) {
      return true; // the added node adds a new group.
    }
    return false; // removing delHint reduces the number of groups.
  }

  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    final Map<String, List<DatanodeInfo>> rackMap = new HashMap<>();
    final List<DatanodeInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeInfo> exactlyOne = new ArrayList<>();
    splitNodesWithRack(locs, locs, rackMap, moreThanOne, exactlyOne);
    return notReduceNumOfGroups(moreThanOne, source, target);
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If only 1 rack, pick all. If 2 racks, pick all that have more than
   * 1 replicas on the same rack; if no such replicas, pick all.
   * If 3 or more racks, pick all.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    Collection<DatanodeStorageInfo> ret = new ArrayList<>();
    if (rackMap.size() == 2) {
      for (List<DatanodeStorageInfo> dsi : rackMap.values()) {
        if (dsi.size() >= 2) {
          ret.addAll(dsi);
        }
      }
    }
    if (ret.isEmpty()) {
      // Return all replicas if rackMap.size() != 2
      // or rackMap.size() == 2 but no shared replicas on any rack
      ret.addAll(moreThanOne);
      ret.addAll(exactlyOne);
    }
    return ret;
  }

  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }
}

