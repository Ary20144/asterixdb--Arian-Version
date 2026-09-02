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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * [Memory-adaptive sort] Replays a fixed trace of broker decisions, so an experiment can pin down
 * <em>exactly</em> when memory moves and by how much. This is what makes a scenario like "victim at
 * 25% of accumulation, reclaiming half" reproducible rather than a lucky draw.
 * <p>
 * Trace format -- one directive per line, {@code #} starts a comment:
 *
 * <pre>
 * # callIndex , action , amount
 * 5           , reclaim, 0.5     # on the 5th broker interaction, take half the budget
 * 12          , grant  , 1.0     # on the 12th, double it
 * 20          , deny   , 0
 * </pre>
 *
 * {@code callIndex} counts <em>all</em> broker interactions for this operator, in order
 * ({@code onStatusUpdate} and {@code requestMore} together), starting at 1. Any call whose index
 * is not in the trace returns 0 -- "not a victim" for a status update, "denied" for a request.
 * {@code amount} is a fraction of the operator's current budget.
 * <p>
 * Note the counter is <em>per operator instance</em>: each sort operator replays the trace from the
 * start, which keeps a multi-partition run deterministic.
 */
public class ScriptedMemoryBroker implements IMemoryBroker {

    private static final class Directive {
        final boolean reclaim;
        final double fraction;

        Directive(boolean reclaim, double fraction) {
            this.reclaim = reclaim;
            this.fraction = fraction;
        }
    }

    private final Map<Long, Directive> script = new HashMap<>();
    private long calls;

    public ScriptedMemoryBroker(String path) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int hash = line.indexOf('#');
                if (hash >= 0) {
                    line = line.substring(0, hash);
                }
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 2) {
                    throw new IOException("Malformed broker script line: " + line);
                }
                long index = Long.parseLong(parts[0].trim());
                String action = parts[1].trim().toLowerCase();
                double amount = parts.length > 2 ? Double.parseDouble(parts[2].trim()) : 0;
                switch (action) {
                    case "reclaim":
                        script.put(index, new Directive(true, amount));
                        break;
                    case "grant":
                        script.put(index, new Directive(false, amount));
                        break;
                    case "deny":
                        script.put(index, new Directive(false, 0));
                        break;
                    default:
                        throw new IOException("Unknown broker script action '" + action + "' in line: " + line);
                }
            }
        }
    }

    @Override
    public long onStatusUpdate(MemoryStatus status) {
        Directive d = next();
        // only a reclaim is meaningful on the polled path; a grant is delivered via requestMore
        return (d != null && d.reclaim) ? -frames(status, d.fraction) : 0;
    }

    @Override
    public long requestMore(MemoryStatus status) {
        Directive d = next();
        if (d == null) {
            return 0; // denied
        }
        long amount = frames(status, d.fraction);
        return d.reclaim ? -amount : amount;
    }

    private Directive next() {
        return script.get(++calls);
    }

    private static long frames(MemoryStatus status, double fraction) {
        long budget = status.easyFrames + status.mediumFrames + status.hardFrames;
        return Math.max(1, Math.round(budget * fraction));
    }
}
