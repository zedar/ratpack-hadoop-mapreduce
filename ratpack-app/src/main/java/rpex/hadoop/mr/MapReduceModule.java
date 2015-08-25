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

import com.google.inject.Provides;
import com.google.inject.Scopes;
import ratpack.guice.ConfigurableModule;
import rpex.hadoop.mr.internal.DefaultMapReduceService;
import rpex.hadoop.mr.topn.TopNService;
import rpex.hadoop.mr.topn.internal.DefaultTopNService;

import javax.inject.Singleton;

/**
 * Provides configuration for Hadoop's map reduce services, endpoints.
 */
public class MapReduceModule extends ConfigurableModule<MapReduceConfig> {

  @Override
  protected void configure() {
    bind(MapReduceEndpoints.class).in(Scopes.SINGLETON);
  }

  /**
   * Provides default implementation of the {@link MapReduceService} interface.
   * @param config a mapreduce configuration
   * @return the singleton for {@link MapReduceService} default implementation
   */
  @Provides
  @Singleton
  public MapReduceService mapReduceService(final MapReduceConfig config) {
    return new DefaultMapReduceService(config);
  }

  /**
   * Provides default implementation of the {@link TopNService} interface.
   *
   * @param mapReduceService a map reduce service providing map-reduce infrastructure
   * @return the singleton for {@link TopNService} implementation.
   */
  @Provides
  @Singleton
  public TopNService topNService(MapReduceService mapReduceService) {
    return new DefaultTopNService(mapReduceService);
  }
}
