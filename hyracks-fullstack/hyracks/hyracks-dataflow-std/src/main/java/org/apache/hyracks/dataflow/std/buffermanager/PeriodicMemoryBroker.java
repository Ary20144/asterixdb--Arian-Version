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

/**
 * [Memory-adaptive sort] Deterministic broker that acts every Nth broker interaction.
 * <p>
 * This is the workhorse for the sweepable experiments: fixing the workload and varying
 * {@code period} gives a clean "how often does memory move" axis, which is exactly what E3
 * (shrink cost) and E4 (run-count explosion) need. Because it counts calls rather than using
 * randomness, two runs with the same configuration issue the identical sequence of commands.
 * <p>
 * The amount is expressed as a <em>fraction of the operator's current budget</em> rather than an
 * absolute frame count, so a single configuration behaves sensibly across different
 * {@code compiler.sortmemory} settings.
 */
public class PeriodicMemoryBroker implements IMemoryBroker {

    /** What the broker does when a period elapses. */
    public enum Action {
        RECLAIM, // take memory away (-N)
        GRANT // give memory (+N)
    }

    private final int period; // act on every Nth broker interaction; <= 0 disables
    private final Action action;
    private final double fraction; // fraction of current budget to move
    private long calls;

    public PeriodicMemoryBroker(int period, Action action, double fraction) {
        this.period = period;
        this.action = action;
        this.fraction = fraction;
    }

    @Override
    public long onStatusUpdate(MemoryStatus status) {
        // Only RECLAIM is meaningful here: this hook sets the victim flag the operator polls.
        // A GRANT is delivered through requestMore(), which is the operator asking for more.
        long amount = tick(status);
        return action == Action.RECLAIM ? amount : 0;
    }

    @Override
    public long requestMore(MemoryStatus status) {
        long amount = tick(status);
        if (amount == 0) {
            return 0; // denied -> operator spills, budget unchanged
        }
        return amount;
    }

    /** Advances the call counter and returns the signed frame delta, or 0 on a non-acting call. */
    private long tick(MemoryStatus status) {
        if (period <= 0) {
            return 0;
        }
        boolean act = (++calls % period) == 0;
        if (!act) {
            return 0;
        }
        long budget = status.easyFrames + status.mediumFrames + status.hardFrames;
        long delta = Math.max(1, Math.round(budget * fraction));
        return action == Action.RECLAIM ? -delta : delta;
    }
}
