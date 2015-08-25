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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.func.Action;
import ratpack.handling.Chain;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import rpex.hadoop.mr.topn.TopNService;
import rpex.hadoop.mr.topn.dto.CalcTopN;
import rpex.hadoop.mr.topn.model.TimeInterval;

import javax.inject.Inject;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import static ratpack.jackson.Jackson.json;
import static ratpack.jackson.Jackson.fromJson;

/**
 * {@code /mr} endpoints chain. Executes different map reduce algorithms
 */
public class MapReduceEndpoints implements Action<Chain> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceEndpoints.class);

  private final TopNService topNService;

  @Inject
  public MapReduceEndpoints(TopNService topNService) {
    this.topNService = topNService;
  }

  @Override
  public void execute(Chain chain) throws Exception {
    chain
      .path("top/:n?", new Handler() {  // :n? means :n parameter is optional
        @Override
        public void handle(Context ctx) throws Exception {
          Integer topN = Integer.valueOf(ctx.getPathTokens().getOrDefault("n", "10"));
          LOGGER.debug("Starting mapreduce: TopN for N={}", topN);
          ctx.byMethod(byMethodSpec -> byMethodSpec
            .get(() -> {
              ctx.render(json(CalcTopN.of(topN, TimeInterval.of(LocalDate.now().toString(), LocalDate.now().plus(1, ChronoUnit.DAYS).toString()))));
            })
            .post(() -> {
              ctx.parse(fromJson(CalcTopN.class))
                .onNull(() -> {
                  ctx.render(json(Integer.valueOf(-1)));
                })
                .then(ctn -> {
                  topNService
                    .apply(ctn.getLimit(), ctn.getTimeInterval(), "input", "output")
                    .map(r -> json(r))
                    .then(ctx::render);
                  //ctx.render(json(ctn.getLimit()));
                });
            })
          );
        }
      });
  }
}
