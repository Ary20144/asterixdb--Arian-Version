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

import java.util.Random;

/**
 * [Memory-adaptive sort] Shell/template broker: ignores the {@link MemoryStatus} and answers randomly.
 * This is the simulateVictim() / simulateGrantMore() logic that used to live inside
 * AbstractExternalSortRunGenerator, moved here so the broker is a single, swappable component.
 * An intelligent broker will replace this class while keeping the same {@link IMemoryBroker} contract.
 */
public class RandomMemoryBroker implements IMemoryBroker {

    private final double victimProbability;
    // [NLJ experiments, knob 2] fraction of the CURRENT budget demanded per victim event
    // (-Dhyracks.sort.broker.victimFraction, default 0.5 = the original halving behavior).
    private final double victimFraction;
    private final Random random;

    public RandomMemoryBroker(double victimProbability, long seed) {
        this(victimProbability, 0.5, seed); // original behavior: demand half
    }

    public RandomMemoryBroker(double victimProbability, double victimFraction, long seed) {
        this.victimProbability = victimProbability;
        this.victimFraction = victimFraction;
        this.random = new Random(seed); // fixed seed --> reproducible experiment
    }

    /** knob 2: -N frames = victimFraction of the current budget, at least one frame. */
    private long victimDemand(MemoryStatus status) {
        return -Math.max(1, Math.round(currentBudgetFrames(status) * victimFraction));
    }

    @Override
    public long onStatusUpdate(MemoryStatus status) {
        // ignores status for now; randomly victimize by reclaiming victimFraction of the budget.
        if (random.nextDouble() < victimProbability) {
            return victimDemand(status); // -N: give N frames back
        }
        return 0; // not a victim
    }

    @Override
    public long requestMore(MemoryStatus status) {
        // ignores status for now; reproduces the old victim / grant / denied outcomes as signed frames.
        long budget = currentBudgetFrames(status);
        if (random.nextDouble() < victimProbability) {
            return victimDemand(status); // "you're actually a victim"
        }
        if (random.nextDouble() < (1.0 - victimProbability)) {
            return budget; // grant: +budget frames (doubles the budget)
        }
        return 0; // denied
    }

    // The operator's current budget in frames is easy (unused) + medium + hard (loaded).
    private static long currentBudgetFrames(MemoryStatus status) {
        return status.easyFrames + status.mediumFrames + status.hardFrames;
    }
}
