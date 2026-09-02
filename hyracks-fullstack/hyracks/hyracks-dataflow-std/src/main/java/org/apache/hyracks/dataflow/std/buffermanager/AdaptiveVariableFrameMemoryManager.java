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
 * [Memory-adaptive sort] {@link VariableFrameMemoryManager} + a broker relay (the sort's buffer manager).
 * Adds nothing to the frame-management behavior; it simply holds an {@link IMemoryBroker} and forwards
 * the operator's {@link MemoryStatus} to it (see {@link IBrokerConduit}). It does NOT spill or release
 * memory -- the operator decides how to act on the broker's command. This is the single point through
 * which the sort talks to the broker.
 */
public class AdaptiveVariableFrameMemoryManager extends VariableFrameMemoryManager implements IBrokerConduit {

    private final IMemoryBroker broker;
    private long reclaimDemand; // broker-set flag: 0 = not a victim, -N = give N frames back

    public AdaptiveVariableFrameMemoryManager(IFramePool framePool, IFrameFreeSlotPolicy freeSlotPolicy,
            IMemoryBroker broker) {
        super(framePool, freeSlotPolicy);
        this.broker = broker;
    }

    @Override
    public void reportStatus(MemoryStatus status) {
        // fire-and-forget from the operator's view; the (random) broker sets our reclaim demand here.
        reclaimDemand = broker.onStatusUpdate(status);
    }

    @Override
    public long getReclaimDemand() {
        return reclaimDemand; // local, non-blocking read of the broker-set flag
    }

    @Override
    public long requestMore(MemoryStatus status) {
        return broker.requestMore(status); // synchronous
    }
}
