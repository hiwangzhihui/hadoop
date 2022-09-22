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
package org.apache.hadoop.hdfs.util;

import static org.apache.hadoop.util.Time.monotonicNow;

/** 
 * a class to throttle the data transfers.
 * This class is thread safe. It can be shared by multiple threads.
 * The parameter bandwidthPerSec specifies the total bandwidth shared by
 * threads.
 * 限流公式：
 *  bytesPerPeriod = bytesPerSecond*period/1000
 */
public class DataTransferThrottler {
  private final long period;          // period over which bw is imposed 计数周期 500ms
  private final long periodExtension; // Max period over which bw accumulates. 累计计数最大周期
  private long bytesPerPeriod;  // total number of bytes can be sent in each period  一个周期内最大能发送的带宽数据量
  private long curPeriodStart;  // current period starting time  一个周期的开始时间
  private long curReserve;      // remaining bytes can be sent in the period 一个周期内剩余能发送的带宽数据量
  private long bytesAlreadyUsed; // 当前周期已使用的带宽数据量

  /** Constructor 
   * @param bandwidthPerSec bandwidth allowed in bytes per second. 
   */
  public DataTransferThrottler(long bandwidthPerSec) {
    this(500, bandwidthPerSec);  // by default throttling period is 500ms 
  }

  /**
   * Constructor
   * @param period in milliseconds. Bandwidth is enforced over this
   *        period.
   * @param bandwidthPerSec bandwidth allowed in bytes per second.
   *                         dfs.datanode.balance.bandwidthPerSec 限流 10MB
   */
  public DataTransferThrottler(long period, long bandwidthPerSec) {
    this.curPeriodStart = monotonicNow();
    this.period = period;
    this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
    this.periodExtension = period*3;
  }

  /**
   * @return current throttle bandwidth in bytes per second.
   */
  public synchronized long getBandwidth() {
    return bytesPerPeriod*1000/period;
  }
  
  /**
   * Sets throttle bandwidth. This takes affect latest by the end of current
   * period.
   */
  public synchronized void setBandwidth(long bytesPerSecond) {
    if ( bytesPerSecond <= 0 ) {
      throw new IllegalArgumentException("" + bytesPerSecond);
    }
    bytesPerPeriod = bytesPerSecond*period/1000;
  }
  
  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.
   *
   * @param numOfBytes
   *     number of bytes sent/received since last time throttle was called
   */
  public synchronized void throttle(long numOfBytes) {
    throttle(numOfBytes, null);
  }

  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.  Allows for optional external cancelation.
   *
   * @param numOfBytes  当前线程传输的数据量大小
   *     number of bytes sent/received since last time throttle was called
   * @param canceler
   *     optional canceler to check for abort of throttle
   */
  public synchronized void throttle(long numOfBytes, Canceler canceler) {
    if ( numOfBytes <= 0 ) {
      return;
    }
    //更新当前带宽余量
    curReserve -= numOfBytes;
    //更新当前带宽已使用量
    bytesAlreadyUsed += numOfBytes;

    while (curReserve <= 0) { //当余量小等于 0 ，则阻塞等待下一个周期
      if (canceler != null && canceler.isCancelled()) {
        return;
      }
      long now = monotonicNow();
      //一个周期截止时间
      long curPeriodEnd = curPeriodStart + period;

      if ( now < curPeriodEnd ) { //带宽资源提前使用完了
        // Wait for next period so that curReserve can be increased.
        try {
          wait( curPeriodEnd - now );
        } catch (InterruptedException e) {
          // Abort throttle and reset interrupted status to make sure other
          // interrupt handling higher in the call stack executes.
          Thread.currentThread().interrupt();
          break;
        }
      } else if ( now <  (curPeriodStart + periodExtension)) { //带宽资源在 periodExtension 范围内用完
        curPeriodStart = curPeriodEnd; //更新起止时间
        curReserve += bytesPerPeriod;  //更新剩余量
      } else { //带宽资源在 periodExtension 之后才用完，长时间没用完
        // discard the prev period. Throttler might not have
        // been used for a long time.
        curPeriodStart = now; //如果长时间没时间节流器，则重置节流器
        curReserve = bytesPerPeriod - bytesAlreadyUsed; //
      }
    }

    bytesAlreadyUsed -= numOfBytes; // TODO 为什么要 - numOfBytes ？
  }
}
