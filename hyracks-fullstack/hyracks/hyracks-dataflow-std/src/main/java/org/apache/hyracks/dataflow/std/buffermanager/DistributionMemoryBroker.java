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
 * [Memory-adaptive sort] Randomized broker whose <em>amounts</em> are drawn from a continuous
 * distribution rather than always being "half the budget" like {@link RandomMemoryBroker}.
 * <p>
 * Two distributions are supported:
 * <ul>
 * <li>{@code NORMAL} -- amount fraction ~ N(mean, stddev)</li>
 * <li>{@code T} -- amount fraction ~ mean + stddev * t(df); heavier tails, so occasional large
 * grants/reclaims occur. Useful for showing the mechanism copes with outliers, not just with
 * well-behaved average-sized moves.</li>
 * </ul>
 * The drawn fraction is clamped to [0, 1] -- a broker can take at most the whole budget, and a
 * negative draw is treated as "no move" rather than an inverted action.
 * <p>
 * Seeded, so a given configuration is reproducible.
 */
public class DistributionMemoryBroker implements IMemoryBroker {

    public enum Distribution {
        NORMAL,
        T
    }

    private final double victimProbability;
    private final Distribution distribution;
    private final double mean;
    private final double stddev;
    private final int degreesOfFreedom;
    private final Random random;

    public DistributionMemoryBroker(double victimProbability, Distribution distribution, double mean, double stddev,
            int degreesOfFreedom, long seed) {
        this.victimProbability = victimProbability;
        this.distribution = distribution;
        this.mean = mean;
        this.stddev = stddev;
        this.degreesOfFreedom = Math.max(1, degreesOfFreedom);
        this.random = new Random(seed);
    }

    @Override
    public long onStatusUpdate(MemoryStatus status) {
        if (random.nextDouble() < victimProbability) {
            long delta = drawFrames(status);
            return delta > 0 ? -delta : 0;
        }
        return 0;
    }

    @Override
    public long requestMore(MemoryStatus status) {
        if (random.nextDouble() < victimProbability) {
            long delta = drawFrames(status);
            return delta > 0 ? -delta : 0; // "you're actually a victim"
        }
        long delta = drawFrames(status);
        return delta; // > 0 granted, 0 denied
    }

    /** Draws a non-negative frame count from the configured distribution, clamped to the budget. */
    private long drawFrames(MemoryStatus status) {
        long budget = status.easyFrames + status.mediumFrames + status.hardFrames;
        double fraction = mean + stddev * sample();
        if (fraction <= 0) {
            return 0;
        }
        if (fraction > 1.0) {
            fraction = 1.0;
        }
        return Math.round(budget * fraction);
    }

    /** A standard draw: N(0,1), or Student's t with the configured degrees of freedom. */
    private double sample() {
        double z = random.nextGaussian();
        if (distribution == Distribution.NORMAL) {
            return z;
        }
        // t(df) = Z / sqrt(chi2(df) / df), with chi2(df) = sum of df squared standard normals.
        double chiSquare = 0;
        for (int i = 0; i < degreesOfFreedom; i++) {
            double n = random.nextGaussian();
            chiSquare += n * n;
        }
        if (chiSquare <= 0) {
            return z;
        }
        return z / Math.sqrt(chiSquare / degreesOfFreedom);
    }
}
