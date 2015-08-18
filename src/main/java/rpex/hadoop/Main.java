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

package rpex.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import ratpack.guice.Guice;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.handling.RequestId;
import ratpack.handling.ResponseTimer;
import ratpack.handling.internal.UuidBasedRequestIdGenerator;
import ratpack.jackson.Jackson;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import rpex.hadoop.mr.MapReduceConfig;
import rpex.hadoop.mr.MapReduceEndpoints;
import rpex.hadoop.mr.MapReduceModule;

/**
 * Starting point for the hadoop analysis server.
 */
public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String... args) throws Exception {
    LOGGER.debug("STARTING...");
    ObjectMapper objectMapper = new ObjectMapper();
    RatpackServer.start(spec -> spec
      .serverConfig(builder -> builder
          .baseDir(BaseDir.find("application.properties"))
          .props(Main.class.getClassLoader().getResource("application.properties"))
          .env().sysProps()
          .require("/hadoop", MapReduceConfig.class)
      )
      .registry(Guice.registry(bindingsSpec -> {
        bindingsSpec
          .bindInstance(ResponseTimer.decorator())
          .module(MapReduceModule.class, config -> config
              .copyOf(bindingsSpec.getServerConfig().get(MapReduceConfig.class))
          );
        Jackson.Init.register(bindingsSpec, objectMapper, objectMapper.writerWithDefaultPrettyPrinter());
      }))
      .handlers(chain -> chain
        .all(new Handler() {
          @Override
          public void handle(Context ctx) throws Exception {
            MDC.put("clientIP", ctx.getRequest().getRemoteAddress().getHostText());
            RequestId.Generator generator = ctx.maybeGet(RequestId.Generator.class).orElse(UuidBasedRequestIdGenerator.INSTANCE);
            RequestId requestId = generator.generate(ctx);
            ctx.getRequest().add(RequestId.class, requestId);
            MDC.put("requestId", requestId.getId());

            LOGGER.debug("ALL CALLED");

            ctx.next();
          }
        })
        .prefix("v1", chain1 -> chain1
            .get("api-def", ctx -> {
              LOGGER.debug("GET API_DEF.JSON");
              ctx.render(ctx.file("public/apidef/api-def.json"));
            })
            .prefix("mr", MapReduceEndpoints.class)
        )
      )
    );
  }
}
