/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.buffermanager;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * [Memory-adaptive sort] Builds the {@link IMemoryBroker} for a sort operator from system properties,
 * so a single deployed jar can run every experimental arm without a rebuild. Set these on the NC JVM
 * via {@code jvm.args} under {@code [nc]} in {@code cc.conf} -- note that {@code JAVA_OPTS} is NOT
 * propagated to NC processes by the NCService.
 *
 * <pre>
 * hyracks.sort.broker = random | none | periodic | scripted | distribution   (default: random)
 *
 *   none         inert; never grants or reclaims. The "no harm" control arm.
 *   random       existing shell behavior: victim/grant/deny at fixed probability, amount = half budget
 *                  .victimProbability (0.3)   .seed (0)
 *   periodic     deterministic: act every Nth broker interaction
 *                  .period (10)   .action (reclaim|grant)   .fraction (0.5)
 *   scripted     replay a trace file of exact decisions
 *                  .script (path, required)
 *   distribution amounts drawn from a normal or t distribution
 *                  .victimProbability (0.3)  .distribution (normal|t)  .mean (0.5)
 *                  .stddev (0.15)  .df (5)   .seed (0)
 * </pre>
 *
 * Every property is prefixed {@code hyracks.sort.broker}. The chosen policy is logged at
 * construction so a run's configuration is recoverable from its log alone.
 */
public class MemoryBrokerFactory {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String PREFIX = "hyracks.sort.broker";

    private MemoryBrokerFactory() {
    }

    public static IMemoryBroker create() {
        String policy = System.getProperty(PREFIX, "random").trim().toLowerCase();
        IMemoryBroker broker = build(policy);
        LOGGER.info("adaptive-sort-broker: policy={} impl={}", policy, broker.getClass().getSimpleName());
        return broker;
    }

    private static IMemoryBroker build(String policy) {
        switch (policy) {
            case "none":
                return new NoOpMemoryBroker();
            case "periodic": {
                int period = intProp("period", 10);
                String action = strProp("action", "reclaim").toUpperCase();
                double fraction = doubleProp("fraction", 0.5);
                return new PeriodicMemoryBroker(period, PeriodicMemoryBroker.Action.valueOf(action), fraction);
            }
            case "scripted": {
                String path = strProp("script", null);
                if (path == null) {
                    throw new IllegalArgumentException(PREFIX + ".script must be set for the scripted broker");
                }
                try {
                    return new ScriptedMemoryBroker(path);
                } catch (IOException e) {
                    // Fail loudly: silently falling back would silently invalidate an experiment.
                    throw new IllegalStateException("Unable to read broker script: " + path, e);
                }
            }
            case "distribution": {
                DistributionMemoryBroker.Distribution dist =
                        DistributionMemoryBroker.Distribution.valueOf(strProp("distribution", "normal").toUpperCase());
                return new DistributionMemoryBroker(doubleProp("victimProbability", 0.3), dist, doubleProp("mean", 0.5),
                        doubleProp("stddev", 0.15), intProp("df", 5), longProp("seed", 0));
            }
            case "random":
            default:
                return new RandomMemoryBroker(doubleProp("victimProbability", 0.3), longProp("seed", 0));
        }
    }

    private static String strProp(String name, String defaultValue) {
        return System.getProperty(PREFIX + "." + name, defaultValue);
    }

    private static int intProp(String name, int defaultValue) {
        return Integer.parseInt(System.getProperty(PREFIX + "." + name, Integer.toString(defaultValue)));
    }

    private static long longProp(String name, long defaultValue) {
        return Long.parseLong(System.getProperty(PREFIX + "." + name, Long.toString(defaultValue)));
    }

    private static double doubleProp(String name, double defaultValue) {
        return Double.parseDouble(System.getProperty(PREFIX + "." + name, Double.toString(defaultValue)));
    }
}
