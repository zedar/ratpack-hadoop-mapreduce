/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package rpex.hadoop.mr;

import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * The configuration for {@link MapReduceModule}. Provides Hadoop's configuration settings.
 */
@NoArgsConstructor
@ToString
public class MapReduceConfig {
  private String user;
  private String yarnRMHost;
  private String yarnRMPort;
  private String yarnRMSchedulerHost;
  private String yarnRMSchedulerPort;
  private String mapReduceType;
  private String fileSystemHost;
  private String fileSystemPort;

  /**
   * Hadoop's users used in map reduce execution.
   * @return the hadoop user
   */
  public String getUser() {
    return user;
  }

  /**
   * Sets hadoop user.
   *
   * @param user the hadoop user
   * @return this
   */
  public MapReduceConfig user(String user) {
    this.user = user;
    return this;
  }

  /**
   * Hadoop YARN resource manager host
   * @return
   */
  public String getYarnRMHost() {
    return yarnRMHost;
  }

  /**
   * Sets Hadoop's YARN resource manager host
   * @param yarnRMHost the YARN resource manager host
   * @return this
   */
  public MapReduceConfig yarnRMHost(String yarnRMHost) {
    this.yarnRMHost = yarnRMHost;
    return this;
  }

  /**
   * Hadoop YARN resource manager port
   * @return the YARN resource manager port
   */
  public String getYarnRMPort() {
    return yarnRMPort;
  }

  /**
   * Sets YARN resource manager port
   * @param yarnRMPort the YARN resource manager port
   * @return this
   */
  public MapReduceConfig yarnRMPort(String yarnRMPort) {
    this.yarnRMPort = yarnRMPort;
    return this;
  }

  /**
   * Hadoop YARN resource manager address.
   * @return the address of the YARN resource manager
   */
  public String getYarnRMAddress() {
    return getYarnRMHost() + ":" + getYarnRMPort();
  }

  /**
   * Hadoop YARN resource manager scheduler host
   * @return the host name for YARN resource manager scheduler
   */
  public String getYarnRMSchedulerHost() {
    return yarnRMSchedulerHost;
  }

  /**
   * Sets YARN resource manager scheduler host
   * @param yarnRMSchedulerHost the YARN resource manager scheduler host
   * @return this
   */
  public MapReduceConfig yarnRMSchedulerHost(String yarnRMSchedulerHost) {
    this.yarnRMSchedulerHost = yarnRMSchedulerHost;
    return this;
  }

  /**
   * Hadoop YARN resource manager scheduler port.
   * @return the port of the YARN resource manager scheduler
   */
  public String getYarnRMSchedulerPort() {
    return yarnRMSchedulerPort;
  }

  /**
   * Sets YARN resource manager scheduler port
   * @param yarnRMSchedulerPort the YARN resource manager scheduler port
   * @return this
   */
  public MapReduceConfig yarnRMSchedulerPort(String yarnRMSchedulerPort) {
    this.yarnRMSchedulerPort = yarnRMSchedulerPort;
    return this;
  }

  /**
   * Hadoop YARN resource manager scheduler address
   * @return the address of the YARN resource manager scheduler
   */
  public String getYarnRMSchedulerAddress() {
    return getYarnRMSchedulerHost() + ":" + getYarnRMSchedulerPort();
  }

  /**
   * Map reduce type. May be: {@code local} or {@code yarn}
   * @return the type of map reduce
   */
  public String getMapReduceType() {
    return mapReduceType;
  }

  /**
   * Sets map reduce type. One of {@code local} or {@code yarn}
   * @param mapReduceType the map reduce type
   * @return this
   */
  public MapReduceConfig mapReduceType(String mapReduceType) {
    this.mapReduceType = mapReduceType;
    return this;
  }

  /**
   * Hadoop's file system (HDFS) host
   * @return the hadoop file system host
   */
  public String getFileSystemHost() {
    return fileSystemHost;
  }

  /**
   * Sets hadoop file system host
   * @param fileSystemHost the hadoop file system host
   * @return this
   */
  public MapReduceConfig fileSystemHost(String fileSystemHost) {
    this.fileSystemHost = fileSystemHost;
    return this;
  }

  /**
   * Hadoop's file system (HDFS) port
   * @return the hadoop file system port
   */
  public String getFileSystemPort() {
    return fileSystemPort;
  }

  /**
   * Sets hadoop file system port
   * @param fileSystemPort the hadoop file system port
   * @return this
   */
  public MapReduceConfig fileSystemPort(String fileSystemPort) {
    this.fileSystemPort = fileSystemPort;
    return this;
  }

  /**
   * Hadoop file system address.
   * @return the address of the hadoop file system
   */
  public String getFileSystemAddress() {
    return "hdfs://" + getFileSystemHost() + ":" + getFileSystemPort();
  }

  public MapReduceConfig copyOf(final MapReduceConfig config) {
    return this
      .user(config.getUser())
      .yarnRMHost(config.getYarnRMHost())
      .yarnRMPort(config.getYarnRMPort())
      .yarnRMSchedulerHost(config.yarnRMSchedulerHost)
      .yarnRMSchedulerPort(config.yarnRMSchedulerPort)
      .mapReduceType(config.getMapReduceType())
      .fileSystemHost(config.getFileSystemHost())
      .fileSystemPort(config.getFileSystemPort());
  }
}
