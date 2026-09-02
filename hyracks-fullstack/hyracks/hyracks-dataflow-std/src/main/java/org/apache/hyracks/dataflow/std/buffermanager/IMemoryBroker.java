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
 * [Memory-adaptive sort] The broker: the (eventually system-wide) authority that decides memory moves
 * between operators from their {@link MemoryStatus}. It is a shell/randomizer for now; an intelligent
 * broker will use the status to balance memory across concurrent operators.
 * <p>
 * Operators never call the broker directly -- they reach it only through their buffer manager
 * ({@link IBrokerConduit}), so the broker has a single, uniform contact surface for every operator.
 */
public interface IMemoryBroker {

    /**
     * The operator's latest status arrives here (fire-and-forget from the operator's side). Returns the
     * reclaim demand the broker now wants from THIS operator: {@code 0} (not a victim) or a negative
     * number ({@code -N} = give N frames back). In a real broker the demand is computed out-of-band from
     * every operator's status; the random shell decides it here, ignoring the status.
     */
    long onStatusUpdate(MemoryStatus status);

    /**
     * Operator-initiated and synchronous: called when the operator has run out of memory and wants more.
     * Returns {@code +N} (granted N frames), {@code 0} (denied), or {@code -N} (you're actually a victim
     * -- give N frames back).
     */
    long requestMore(MemoryStatus status);
}
