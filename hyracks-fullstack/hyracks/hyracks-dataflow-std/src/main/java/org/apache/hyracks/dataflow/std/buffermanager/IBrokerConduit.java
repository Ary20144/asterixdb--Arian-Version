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
 * [Memory-adaptive sort] Implemented by the (adaptive) buffer managers so that an operator reaches the
 * broker ONLY through its buffer manager -- a single broker contact point shared by every operator.
 * <p>
 * The buffer manager is a pure conduit: it relays the operator's {@link MemoryStatus} to the broker and
 * returns the broker's command. It does NOT spill or release memory itself; the operator decides how to
 * act on the command (e.g. flush a run, grow/shrink its budget).
 */
public interface IBrokerConduit {

    /**
     * FIRE-AND-FORGET: hand the operator's latest status up to the broker and return immediately. The
     * broker may (out-of-band) update this operator's reclaim demand, which is read later, locally, via
     * {@link #getReclaimDemand()}. The operator never blocks waiting on a response here.
     */
    void reportStatus(MemoryStatus status);

    /**
     * LOCAL, non-blocking read of the broker-set reclaim demand: {@code 0} (not a victim) or {@code -N}
     * (give N frames back). No broker interaction -- just reads a flag the broker already set.
     */
    long getReclaimDemand();

    /**
     * SYNCHRONOUS: the operator ran out of memory and asks for more. Returns {@code +N} (granted),
     * {@code 0} (denied), or {@code -N} (you're actually a victim -- give N frames back).
     */
    long requestMore(MemoryStatus status);
}
