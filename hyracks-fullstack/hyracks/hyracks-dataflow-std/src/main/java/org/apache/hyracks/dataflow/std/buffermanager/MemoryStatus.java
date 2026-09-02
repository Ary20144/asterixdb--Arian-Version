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
 * [Memory-adaptive sort] The 3-tier memory report an operator hands to the broker (relayed by its
 * buffer manager). It describes how much memory the operator can give back and at what cost:
 * <ul>
 * <li>{@code easyFrames}: unused budget -- releasing it is free (no work lost, no re-fetch).</li>
 * <li>{@code mediumFrames}: loaded AND already sorted -- releasing means spilling an already-sorted
 * run: it slows the operator (smaller run) but requires no re-IO.</li>
 * <li>{@code hardFrames}: loaded but NOT yet sorted -- releasing it forces the data to be re-fetched
 * from IO later; the most expensive tier.</li>
 * </ul>
 * The current (random) broker ignores these; an intelligent broker will use them to decide which
 * operator should give memory back.
 */
public class MemoryStatus {

    public final long easyFrames;
    public final long mediumFrames;
    public final long hardFrames;

    public MemoryStatus(long easyFrames, long mediumFrames, long hardFrames) {
        this.easyFrames = easyFrames;
        this.mediumFrames = mediumFrames;
        this.hardFrames = hardFrames;
    }

    @Override
    public String toString() {
        return "MemoryStatus[easy=" + easyFrames + ", medium=" + mediumFrames + ", hard=" + hardFrames + "]";
    }
}
