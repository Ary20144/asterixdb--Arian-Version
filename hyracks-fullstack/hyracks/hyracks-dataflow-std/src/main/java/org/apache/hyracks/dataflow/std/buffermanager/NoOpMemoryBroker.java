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
 * [Memory-adaptive sort] Inert broker: never grants, never reclaims, never denies-with-victim.
 * <p>
 * This is the control arm for the "no harm" experiment (E1 in {@code paper_experiment_plan.md}):
 * with this broker the operator's budget must never move, so any performance difference against
 * stock AsterixDB is attributable to the sorter itself (bucket sort + merge cascade), not to
 * memory adaptation.
 * <p>
 * {@code requestMore} returns 0 = "denied", which routes the operator to its ordinary
 * spill-and-continue path -- exactly what stock AsterixDB does when the sorter fills up.
 */
public class NoOpMemoryBroker implements IMemoryBroker {

    @Override
    public long onStatusUpdate(MemoryStatus status) {
        return 0; // never a victim
    }

    @Override
    public long requestMore(MemoryStatus status) {
        return 0; // always denied -> operator spills, budget unchanged
    }
}
