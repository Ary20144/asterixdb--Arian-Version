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
package org.apache.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.AdaptiveVariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IBrokerConduit;
import org.apache.hyracks.dataflow.std.buffermanager.MemoryBrokerFactory;
import org.apache.hyracks.dataflow.std.buffermanager.MemoryStatus;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NestedLoopJoin {
    //creating the logger for test:
    private static final Logger LOGGER = LogManager.getLogger();
    // Note: Min memory budget should be less than {@code AbstractJoinPOperator.MIN_FRAME_LIMIT_FOR_JOIN}
    // Inner join: 1 frame for the outer input side, 1 frame for the inner input side, 1 frame for the output
    private static final int MIN_FRAME_BUDGET_INNER_JOIN = 3;
    // Outer join extra: Add 1 frame for the {@code outerMatchLOJ} bitset
    private static final int MIN_FRAME_BUDGET_OUTER_JOIN = MIN_FRAME_BUDGET_INNER_JOIN + 1;
    // Outer join needs 1 bit per each tuple in the outer side buffer
    private static final int ESTIMATE_AVG_TUPLE_SIZE = 128;

    private final FrameTupleAccessor accessorInner;
    private final FrameTupleAccessor accessorOuter;
    private final FrameTupleAppender appender;
    private ITuplePairComparator tpComparator;
    private final IFrame outBuffer;
    private final IFrame innerBuffer;
    private final VariableFrameMemoryManager outerBufferMngr;

    private final RunFileWriter runFileWriter;
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder missingTupleBuilder;
    // Added for handling correct calling of recursive calls
    // (in OptimizedHybridHashJoin) that cause role-reversal
    private final boolean isReversed;
    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);
    private final BitSet outerMatchLOJ;

    //added flag to check victim
    private final VariableFramePool framePool;
    private int sPassCount = 0; // completed S-passes, replays included (was drainCount)
    //broker connection (Ameen's abstraction): the operator reaches the broker ONLY through its buffer
    //manager (IBrokerConduit, a pure conduit that never spills or releases by itself); the policy
    //(none/random/periodic/scripted/distribution) is chosen at runtime by MemoryBrokerFactory
    //(-Dhyracks.sort.broker=...), so one jar covers every experiment arm.
    private final IBrokerConduit broker;
    private final String brokerPolicy = System.getProperty("hyracks.sort.broker", "random");
    private int victimCount = 0; // victim requests served: fill-time, block-boundary, and mid-pass
    private int grantCount = 0;
    private int rejectCount = 0;
    private int victimCheckInterval = 100; // the paper's x: victim check every x R frames (was frameInterval)
    private int frameCounter = 0;
    // BENCHMARK KNOBS: the victim/grant/spill DECISIONS moved into the broker policy (MemoryBrokerFactory);
    // what stays here is operator mechanism only. No caps: the grant ceiling (was grantCapBytes) and the
    // release-count cap (was maxBucketReleases) are removed; the operator serves whatever the broker
    // decides, and the only remaining limits are the one-frame correctness floor and the pool's int budget.
    private long pendingReleaseBytes = 0; // the broker's demanded amount for a boundary-victim / whole-block-spill serve
    // BENCHMARK METRICS (reported in SUMMARY)
    private long sFrameReads = 0; // S frames read back from the run file (disk contact incl. replay)
    private long matchCount = 0; // joined pairs appended (replay included) — the true result cardinality
    private long boundaryRespNanosTotal = 0; // fill-time + block-boundary releases: decision -> released (was cp12RespNanosTotal)
    private int boundaryRespEvents = 0; // (was cp12RespEvents)
    private long midPassRespNanosTotal = 0; // mid-pass releases: decision -> released, bucket write included (was cp3RespNanosTotal)
    private int midPassRespEvents = 0; // (was cp3RespEvents)
    private long midPassRequestNanos = 0; // stamp of the most recent mid-pass victim decision (was cp3RequestNanos)
    // MID-PASS RELEASE (was CHECKPOINT 3): release R buckets mid-S-pass so memory frees immediately.
    // Multiple releases per join, unbounded; inner joins only
    // (LOJ needs outerMatchLOJ state spilled too — future work). Each release parks one
    // (file, s_resume_idx) debt: bucket_k x S[j_k..end), repaid independently in completeJoin.
    private IHyracksJobletContext jobletContext; // promoted from ctor param (spill file creation)
    private final List<RunFileWriter> releasedBucketWriters = new ArrayList<>(); // run files of released buckets (was cp3SpillWriters)
    private final List<Integer> rBucketStatus = new ArrayList<>(); // the paper's status array: s_resume_idx per released bucket (was cp3ResumePoints)
    private int replaySResumeIdx = 0; // s_resume_idx of the bucket CURRENTLY replaying (was cp3ResumeInnerFrames)
    private boolean wholeBlockSpillPending = false; // whole-block spill awaiting its post-S-pass serve (was cp3SpillJustHappened)
    private boolean replaying = false; // skip-mode flag, matches the paper's algorithm (was cp3Replaying)
    private int bucketReleaseCount = 0; // mid-pass releases, bucket or whole-block (was cp3SpillCount)
    // BUCKET-BASED vs BASIC design (paper terms):
    // false = Basic Memory-Adaptive BNLJ: idle-only takes; a mid-pass victim spills the WHOLE block
    //         and ends the S-pass early.
    // true  = Bucket-based Memory-Adaptive BNLJ: releases also shrink unallocated headroom, and a
    //         mid-pass victim releases ONLY the demanded buckets (one bucket group = one debt)
    //         while the S-pass continues on the surviving frames. (was bucketedRelease)
    private boolean bucketBased = true;
    // The framePool holds ONLY the outer (R) cache. Of the 3-frame minimum above, the inner (S) reader
    // and the output buffer are allocated OUTSIDE the pool in the constructor — so the pool's own floor
    // is the remaining 1 outer frame. Derived, not hardcoded, so it tracks the minimum if it ever changes.
    private static final int MIN_POOL_FRAMES = MIN_FRAME_BUDGET_INNER_JOIN - 2;

    public NestedLoopJoin(IHyracksJobletContext jobletContext, FrameTupleAccessor accessorOuter,
            FrameTupleAccessor accessorInner, int memBudgetInFrames, boolean isLeftOuter,
            IMissingWriter[] missingWriters) throws HyracksDataException {
        this(jobletContext, accessorOuter, accessorInner, memBudgetInFrames, isLeftOuter, missingWriters, false);
    }

    public NestedLoopJoin(IHyracksJobletContext jobletContext, FrameTupleAccessor accessorOuter,
            FrameTupleAccessor accessorInner, int memBudgetInFrames, boolean isLeftOuter,
            IMissingWriter[] missingWriters, boolean isReversed) throws HyracksDataException {
        this.accessorInner = accessorInner;
        this.accessorOuter = accessorOuter;
        //kept for creating bucket spill files at mid-pass releases
        this.jobletContext = jobletContext;

        this.appender = new FrameTupleAppender();
        this.outBuffer = new VSizeFrame(jobletContext);
        this.innerBuffer = new VSizeFrame(jobletContext);
        this.appender.reset(outBuffer, true);

        int minMemBudgetInFrames = isLeftOuter ? MIN_FRAME_BUDGET_OUTER_JOIN : MIN_FRAME_BUDGET_INNER_JOIN;
        if (memBudgetInFrames < minMemBudgetInFrames) {
            throw new HyracksDataException(ErrorCode.INSUFFICIENT_MEMORY);
        }
        int outerBufferMngrMemBudgetInFrames = memBudgetInFrames - minMemBudgetInFrames + 1;
        int outerBufferMngrMemBudgetInBytes = jobletContext.getInitialFrameSize() * outerBufferMngrMemBudgetInFrames;
        //        this.outerBufferMngr = new VariableFrameMemoryManager(
        //                new VariableFramePool(jobletContext, outerBufferMngrMemBudgetInBytes), FrameFreeSlotPolicyFactory
        //                        .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));
        //changed for testing memory changes
        this.framePool = new VariableFramePool(jobletContext, outerBufferMngrMemBudgetInBytes);
        // [Ameen abstraction] the buffer manager doubles as the operator's single broker contact:
        // a pure conduit that relays MemoryStatus/commands and never spills or releases by itself.
        AdaptiveVariableFrameMemoryManager adaptiveMngr = new AdaptiveVariableFrameMemoryManager(framePool,
                FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT,
                        outerBufferMngrMemBudgetInFrames),
                MemoryBrokerFactory.create());
        this.outerBufferMngr = adaptiveMngr;
        this.broker = adaptiveMngr;

        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
            if (isReversed) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, "Outer join cannot reverse roles");
            }
            int innerFieldCount = this.accessorInner.getFieldCount();
            missingTupleBuilder = new ArrayTupleBuilder(innerFieldCount);
            DataOutput out = missingTupleBuilder.getDataOutput();
            for (int i = 0; i < innerFieldCount; i++) {
                missingWriters[i].writeMissing(out);
                missingTupleBuilder.addFieldEndOffset();
            }
            // Outer join needs 1 bit per each tuple in the outer side buffer
            int outerMatchLOJCardinalityEstimate = outerBufferMngrMemBudgetInBytes / ESTIMATE_AVG_TUPLE_SIZE;
            outerMatchLOJ = new BitSet(Math.max(outerMatchLOJCardinalityEstimate, 1));
        } else {
            missingTupleBuilder = null;
            outerMatchLOJ = null;
        }
        this.isReversed = isReversed;

        FileReference file =
                jobletContext.createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, jobletContext.getIoManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        accessorInner.reset(buffer);
        if (accessorInner.getTupleCount() > 0) {
            runFileWriter.nextFrame(buffer);
        }
    }

    /**
     * Must be called before starting to join to set the right comparator with the right context.
     *
     * @param comparator the comparator to use for comparing the probe tuples against the build tuples
     */
    void setComparator(ITuplePairComparator comparator) {
        tpComparator = comparator;
    }

    /**
     adapted to Ameen's broker abstraction: the coin flips moved into the broker policy
     (RandomMemoryBroker etc.); the operator only reports status and obeys signed-frame commands.
     */

    /** frames -> bytes at the pool's uniform frame size (the broker speaks frames, the pool bytes). */
    private long framesToBytes(long frames) {
        return frames * framePool.getMinFrameSize();
    }

    /** Three-tier supply curve reported to the broker (easy + medium + hard == current budget frames):
     *  easy = idle frames + never-allocated headroom (instant, free to give); medium = live frames the
     *  bucket-based path could release mid-pass (~ms plus a deferred replay); hard = the pool floor. */
    private MemoryStatus buildStatus() {
        long budgetFrames = framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize();
        long liveFrames = outerBufferMngr.getNumFrames();
        long medium = bucketBased ? Math.max(0, liveFrames - MIN_POOL_FRAMES) : 0;
        long hard = liveFrames - medium;
        long easy = Math.max(0, budgetFrames - liveFrames);
        return new MemoryStatus(easy, medium, hard);
    }

    /** Victim action (was releaseOuterMemory): release the broker-demanded bytes from the OUTER (R)
     *  pool, clamped so the pool keeps its floor. Response time is decision -> released: for fill-time
     *  and block-boundary serves the decision is this call; for a mid-pass serve the decision was the
     *  spill (stamped in midPassRequestNanos), so the bucket write is part of the wait. */
    private void serveVictimRequest(long requestBytes, boolean servingMidPass) {
        long t0 = System.nanoTime();
        int current = framePool.getMemoryBudgetBytes();
        int ask = (int) Math.min(requestBytes, current - (long) MIN_POOL_FRAMES * framePool.getMinFrameSize());
        if (ask > 0) {
            int got = framePool.takeUnusedMemory(ask);
            int headroom = 0;
            if (bucketBased && got < ask) {
                // bucket-based tier 2: budget never allocated into frames costs nothing to surrender
                headroom = framePool.shrinkUnallocated(ask - got);
                got += headroom;
            }
            victimCount++;
            if (servingMidPass) {
                midPassRespNanosTotal += System.nanoTime() - midPassRequestNanos;
                midPassRespEvents++;
            } else {
                boundaryRespNanosTotal += System.nanoTime() - t0;
                boundaryRespEvents++;
            }
            LOGGER.info("NLJ-BROKER sPass#{} VICTIM releasing outer budget: asked={} took={} (headroom={}) capNow={}",
                    sPassCount, ask, got, headroom, framePool.getMemoryBudgetBytes());
        } else {
            LOGGER.info("NLJ-BROKER sPass#{} VICTIM-DECLINED cap={} at pool floor", sPassCount, current);
        }
    }

    /** Grow by the broker's grant (+N frames), uncapped. Returns whether the cap grew. The only clamp
     *  is the pool's int byte budget, a mechanical limit of the implementation rather than a policy cap. */
    private boolean growBudget(long grantedFrames) {
        int current = framePool.getMemoryBudgetBytes();
        long target = Math.min(current + framesToBytes(grantedFrames), Integer.MAX_VALUE);
        if (target > current) {
            framePool.updateBudget((int) target);
            grantCount++;
            LOGGER.info("NLJ-BROKER sPass#{} GRANTED {} -> {}", sPassCount, current, (int) target);
            return true;
        }
        rejectCount++;
        LOGGER.info("NLJ-BROKER sPass#{} REJECTED cap stays {}", sPassCount, current);
        return false;
    }

    //helper functions for the mid-pass victim check
    /** Mid-pass victim check (was cp3ReclaimDemandFrames): cheap guards first, then a status report and
     *  a local read of the broker-set reclaim demand. Returns the demanded frames (0 = not a victim);
     *  the demand IS the bucket size (cut-to-order). */
    private int midPassVictimCheck(int sResumeIdx) {
        if (isLeftOuter || replaying || sResumeIdx <= 0) {
            return 0;
        }
        broker.reportStatus(buildStatus());
        long reclaim = broker.getReclaimDemand();
        if (reclaim >= 0) {
            return 0;
        }
        int current = framePool.getMemoryBudgetBytes();
        if (current - framesToBytes(-reclaim) < (long) MIN_POOL_FRAMES * framePool.getMinFrameSize()) {
            return 0; // don't spill for a take the floor would decline
        }
        return (int) -reclaim;
    }

    /** Shared S-pass epilogue (was resetAfterDrain): count, reset, and serve any pending release
     *  (block-boundary victim and/or whole-block spill). */
    private void resetAfterSPass(boolean releaseMemory) throws HyracksDataException {
        sPassCount++;
        outerBufferMngr.reset();
        boolean servingMidPass = wholeBlockSpillPending;
        if (wholeBlockSpillPending) {
            wholeBlockSpillPending = false;
            releaseMemory = true; // the whole-block spill's serve rides this path
        }
        if (releaseMemory) {
            // serve the broker's demanded amount (boundary victim / whole-block spill); half-budget fallback
            long bytes = pendingReleaseBytes > 0 ? pendingReleaseBytes : framePool.getMemoryBudgetBytes() / 2;
            pendingReleaseBytes = 0;
            serveVictimRequest(bytes, servingMidPass);
        }
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        accessorOuter.reset(outerBuffer);
        if (accessorOuter.getTupleCount() <= 0) {
            return;
        }
        frameCounter++;
        // Fill-time victim check (was CHECKPOINT 1): R still filling — give-up side ONLY
        // [Ameen abstraction] every victimCheckInterval frames: fire-and-forget status report, then a local
        // non-blocking read of the broker-set reclaim demand (0 = not a victim, -N = give N frames back)
        if (frameCounter % victimCheckInterval == 0) {
            broker.reportStatus(buildStatus());
            long reclaim = broker.getReclaimDemand();
            if (reclaim < 0) {
                serveVictimRequest(framesToBytes(-reclaim), false);
            }
        }
        if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
            // Block-boundary check (was CHECKPOINT 2): out of memory — one SYNCHRONOUS requestMore
            // decides victim/grant/denied
            long resp = broker.requestMore(buildStatus());
            if (resp < 0) {
                // victim FIRST: finish the S-pass, then the epilogue releases the demanded frames (full yield)
                pendingReleaseBytes = framesToBytes(-resp);
                //                    multiBlockJoin(writer);
                //                    sPassCount++;
                //                    outerBufferMngr.reset();
                //                    releaseHalfOfOuterBudget();
                multiBlockJoin(writer);
                resetAfterSPass(true);
            } else {
                // not victim: the grant came BEFORE spilling — a grant makes the S-pass unnecessary
                if (resp > 0 && growBudget(resp) && outerBufferMngr.insertFrame(outerBuffer) >= 0) {
                    return; // grant absorbed the frame: S-pass avoided
                }
                if (resp == 0) {
                    rejectCount++;
                    LOGGER.info("NLJ-BROKER sPass#{} REJECTED cap stays {}", sPassCount,
                            framePool.getMemoryBudgetBytes());
                }
                //                    multiBlockJoin(writer);              // rejected -> spill as usual
                //                    sPassCount++;
                //                    outerBufferMngr.reset();
                multiBlockJoin(writer);
                resetAfterSPass(false);
            }
            if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
                throw new HyracksDataException("The given outer frame of size:" + outerBuffer.capacity()
                        + " is too big to cache in the buffer. Please choose a larger buffer memory size");
            }
        }

    }

    //    private void multiBlockJoin(IFrameWriter writer) throws HyracksDataException {
    //        int outerBufferFrameCount = outerBufferMngr.getNumFrames();
    //        if (outerBufferFrameCount == 0) {
    //            return;
    //        }
    //        RunFileReader runFileReader = runFileWriter.createReader();
    //        try {
    //            runFileReader.open();
    //            if (isLeftOuter) {
    //                outerMatchLOJ.clear();
    //            }
    //            while (runFileReader.nextFrame(innerBuffer)) {
    //                int outerTupleRunningCount = 0;
    //                for (int i = 0; i < outerBufferFrameCount; i++) {
    //                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
    //                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
    //                            outerBufferInfo.getLength());
    //                    int outerTupleCount = accessorOuter.getTupleCount();
    //                    accessorInner.reset(innerBuffer.getBuffer());
    //                    blockJoin(outerTupleRunningCount, writer);
    //                    outerTupleRunningCount += outerTupleCount;
    //                }
    //            }
    //            if (isLeftOuter) {
    //                int outerTupleRunningCount = 0;
    //                for (int i = 0; i < outerBufferFrameCount; i++) {
    //                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
    //                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
    //                            outerBufferInfo.getLength());
    //                    int outerFrameTupleCount = accessorOuter.getTupleCount();
    //                    appendMissing(outerTupleRunningCount, outerFrameTupleCount, writer);
    //                    outerTupleRunningCount += outerFrameTupleCount;
    //                }
    //            }
    //        } finally {
    //            runFileReader.close();
    //        }
    //    }

    //changed for the mid-pass victim check (the S-pass loop; lock-step: all resident R frames advance through S together)
    private void multiBlockJoin(IFrameWriter writer) throws HyracksDataException {
        int outerBufferFrameCount = outerBufferMngr.getNumFrames();
        if (outerBufferFrameCount == 0) {
            return;
        }
        RunFileReader runFileReader = runFileWriter.createReader();
        int sResumeIdx = 0;
        try {
            runFileReader.open();
            if (isLeftOuter) {
                outerMatchLOJ.clear();
            }
            while (runFileReader.nextFrame(innerBuffer)) {
                sFrameReads++; // every S frame pulled from the run file, including replay-skipped ones
                if (replaying && sResumeIdx < replaySResumeIdx) {
                    // replay mode: these S frames were already joined against the spilled block
                    sResumeIdx++;
                    continue;
                }
                int bucketFrames = midPassVictimCheck(sResumeIdx);
                if (bucketFrames > 0) {
                    if (bucketBased && bucketFrames < outerBufferFrameCount) {
                        // Mid-pass bucket release (was CHECKPOINT 3, bucketed): spill ONLY the demanded
                        // frames (one bucket group = one debt, cut-to-order), free them right here,
                        // and keep scanning the survivors
                        outerBufferFrameCount =
                                releaseBuckets(bucketFrames, sResumeIdx, outerBufferFrameCount);
                    } else {
                        // Whole-block fallback (was CHECKPOINT 3, original): preserve the whole block,
                        // end the pass early; the caller's resetAfterSPass() frees and releases the amount
                        pendingReleaseBytes = framesToBytes(bucketFrames);
                        spillWholeRBlock(sResumeIdx);
                        return;
                    }
                }
                int outerTupleRunningCount = 0;
                for (int i = 0; i < outerBufferFrameCount; i++) {
                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                            outerBufferInfo.getLength());
                    int outerTupleCount = accessorOuter.getTupleCount();
                    accessorInner.reset(innerBuffer.getBuffer());
                    blockJoin(outerTupleRunningCount, writer);
                    outerTupleRunningCount += outerTupleCount;
                }
                sResumeIdx++;
            }
            if (isLeftOuter) {
                int outerTupleRunningCount = 0;
                for (int i = 0; i < outerBufferFrameCount; i++) {
                    BufferInfo outerBufferInfo = outerBufferMngr.getFrame(i, tempInfo);
                    accessorOuter.reset(outerBufferInfo.getBuffer(), outerBufferInfo.getStartOffset(),
                            outerBufferInfo.getLength());
                    int outerFrameTupleCount = accessorOuter.getTupleCount();
                    appendMissing(outerTupleRunningCount, outerFrameTupleCount, writer);
                    outerTupleRunningCount += outerFrameTupleCount;
                }
            }
        } finally {
            runFileReader.close();
        }
    }

    /** Preserve the whole pinned block on disk so reset() can legally free it (was spillCurrentOuterBlock).
     *  Assumes logical frame == whole physical frame (uniform 32KB, verified in this setup).
     *  Each call parks one independent debt: this block x S[sResumeIdx..end). */
    private void spillWholeRBlock(int sResumeIdx) throws HyracksDataException {
        midPassRequestNanos = System.nanoTime(); // broker "request" moment; released in resetAfterSPass
        FileReference file =
                jobletContext.createManagedWorkspaceFile("NLJBlockSpill" + releasedBucketWriters.size() + this.toString());
        RunFileWriter spillWriter = new RunFileWriter(file, jobletContext.getIoManager());
        spillWriter.open();
        int n = outerBufferMngr.getNumFrames();
        for (int i = 0; i < n; i++) {
            spillWriter.nextFrame(outerBufferMngr.getFrame(i, tempInfo).getBuffer());
        }
        spillWriter.close();
        releasedBucketWriters.add(spillWriter);
        rBucketStatus.add(sResumeIdx);
        wholeBlockSpillPending = true;
        bucketReleaseCount++;
        LOGGER.info("NLJ-BROKER sPass#{} BLOCK-SPILL #{}: {} frames after {} inner frames; rest deferred",
                sPassCount, releasedBucketWriters.size(), n, sResumeIdx);
    }

    /** Bucket release, cut-to-order (was spillBucketMidScan): spill only the LAST nFrames of the pinned block
     *  as one debt (this bucket x S[sResumeIdx..end)), free exactly those frames, and
     *  return the surviving frame count so the caller's scan continues on frames 0..survivors-1.
     *  Same 1:1 logical/physical frame assumption as spillWholeRBlock. The release happens
     *  HERE (write + free), so no epilogue serve is needed and the scan is never aborted. */
    private int releaseBuckets(int nFrames, int sResumeIdx, int frameCount)
            throws HyracksDataException {
        midPassRequestNanos = System.nanoTime(); // broker "request" moment; released at end of this method
        FileReference file =
                jobletContext.createManagedWorkspaceFile("NLJBucket" + releasedBucketWriters.size() + this.toString());
        RunFileWriter spillWriter = new RunFileWriter(file, jobletContext.getIoManager());
        spillWriter.open();
        for (int i = frameCount - nFrames; i < frameCount; i++) {
            spillWriter.nextFrame(outerBufferMngr.getFrame(i, tempInfo).getBuffer());
        }
        spillWriter.close();
        releasedBucketWriters.add(spillWriter); // replayed by the existing replayReleasedBuckets, unchanged
        rBucketStatus.add(sResumeIdx);
        bucketReleaseCount++;
        victimCount++;
        List<ByteBuffer> evicted = new ArrayList<>();
        outerBufferMngr.removeTrailingFrames(nFrames, evicted);
        int freed = 0;
        for (ByteBuffer b : evicted) {
            freed += framePool.releaseSpecificFrame(b); // cap + ledger updated per actual bytes
        }
        midPassRespNanosTotal += System.nanoTime() - midPassRequestNanos;
        midPassRespEvents++;
        LOGGER.info(
                "NLJ-BROKER sPass#{} BUCKET-RELEASE #{}: spilled {} frames after {} inner frames, freed={} capNow={}; scan continues on {} frames",
                sPassCount, releasedBucketWriters.size(), nFrames, sResumeIdx, freed,
                framePool.getMemoryBudgetBytes(), frameCount - nFrames);
        return frameCount - nFrames;
    }

    /** Repay every released bucket (was replayCP3Spills): bucket_k x S[j_k..end), each via the normal
     *  insert/S-pass cycle. */
    private void replayReleasedBuckets(IFrameWriter writer) throws HyracksDataException {
        for (int k = 0; k < releasedBucketWriters.size(); k++) {
            replaySResumeIdx = rBucketStatus.get(k); // this bucket's s_resume_idx
            int replayed = 0;
            RunFileReader spillReader = releasedBucketWriters.get(k).createDeleteOnCloseReader();
            IFrame replayFrame = new VSizeFrame(jobletContext);
            replaying = true;
            try {
                outerBufferMngr.reset(); // THE FIX: pool may hold the fully-probed final block (or bucket k-1)
                spillReader.open();
                try {
                    while (spillReader.nextFrame(replayFrame)) {
                        if (outerBufferMngr.insertFrame(replayFrame.getBuffer()) < 0) {
                            multiBlockJoin(writer);
                            resetAfterSPass(false); // honest sPassCount, same epilogue
                            outerBufferMngr.insertFrame(replayFrame.getBuffer());
                        }
                        replayed++;
                    }
                } finally {
                    spillReader.close();
                }
                multiBlockJoin(writer); // last partial debt block
                resetAfterSPass(false);
            } finally {
                replaying = false;
            }
            LOGGER.info("NLJ-BROKER BUCKET-REPLAY {}/{} frames={} sResumeIdx={}", k + 1, releasedBucketWriters.size(),
                    replayed, replaySResumeIdx);
        }
        releasedBucketWriters.clear();
        rBucketStatus.clear();
        replaySResumeIdx = 0;
    }

    private void blockJoin(int outerTupleStartPos, IFrameWriter writer) throws HyracksDataException {
        int outerTupleCount = accessorOuter.getTupleCount();
        int innerTupleCount = accessorInner.getTupleCount();
        for (int i = 0; i < outerTupleCount; ++i) {
            boolean matchFound = false;
            for (int j = 0; j < innerTupleCount; ++j) {
                int c = tpComparator.compare(accessorOuter, i, accessorInner, j);
                if (c == 0) {
                    matchFound = true;
                    matchCount++; // logged in SUMMARY: result cardinality even when the query LIMITs output
                    appendToResults(i, j, writer);
                }
            }
            if (isLeftOuter && matchFound) {
                outerMatchLOJ.set(outerTupleStartPos + i);
            }
        }
    }

    private void appendToResults(int outerTupleId, int innerTupleId, IFrameWriter writer) throws HyracksDataException {
        if (isReversed) {
            appendResultToFrame(accessorInner, innerTupleId, accessorOuter, outerTupleId, writer);
        } else {
            appendResultToFrame(accessorOuter, outerTupleId, accessorInner, innerTupleId, writer);
        }
    }

    private void appendResultToFrame(FrameTupleAccessor accessor1, int tupleId1, FrameTupleAccessor accessor2,
            int tupleId2, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, appender, accessor1, tupleId1, accessor2, tupleId2);
    }

    private void appendMissing(int outerFrameMngrStartPos, int outerFrameTupleCount, IFrameWriter writer)
            throws HyracksDataException {
        int limit = outerFrameMngrStartPos + outerFrameTupleCount;
        for (int outerTuplePos =
                outerMatchLOJ.nextClearBit(outerFrameMngrStartPos); outerTuplePos < limit; outerTuplePos =
                        outerMatchLOJ.nextClearBit(outerTuplePos + 1)) {
            int[] ntFieldEndOffsets = missingTupleBuilder.getFieldEndOffsets();
            byte[] ntByteArray = missingTupleBuilder.getByteArray();
            int ntSize = missingTupleBuilder.getSize();
            int outerAccessorTupleIndex = outerTuplePos - outerFrameMngrStartPos;
            FrameUtils.appendConcatToWriter(writer, appender, accessorOuter, outerAccessorTupleIndex, ntFieldEndOffsets,
                    ntByteArray, 0, ntSize);
        }
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        //        try {
        //            multiBlockJoin(writer);
        //        } finally {
        //            runFileWriter.eraseClosed();
        //        }
        //        appender.write(writer, true);
        try {
            multiBlockJoin(writer);
            if (wholeBlockSpillPending) { // whole-block spill fired during the FINAL S-pass
                resetAfterSPass(false); // flag inside makes it release
            }
            replayReleasedBuckets(writer);
        } finally {
            runFileWriter.eraseClosed();
        }
        appender.write(writer, true);
        LOGGER.info(
                "NLJ-BROKER SUMMARY policy={} knobs[bucketBased={}] sPasses={} victims={} "
                        + "granted={} rejected={} bucketReleases={} sFrameReads={} matches={} "
                        + "outerFramesIn={} outerBytesIn={} innerRunFileBytes={} finalCap={} totalBytesGivenUp={} "
                        + "avgRespMicrosBoundary={} (n={}) avgRespMicrosMidPass={} (n={})",
                brokerPolicy,
                bucketBased, sPassCount, victimCount, grantCount, rejectCount, bucketReleaseCount, sFrameReads,
                matchCount, frameCounter, (long) frameCounter * framePool.getMinFrameSize(),
                runFileWriter.getFileSize(),
                framePool.getMemoryBudgetBytes(), framePool.getGivenBytes(),
                boundaryRespNanosTotal / Math.max(boundaryRespEvents, 1) / 1000, boundaryRespEvents,
                midPassRespNanosTotal / Math.max(midPassRespEvents, 1) / 1000, midPassRespEvents);

    }

    public void releaseMemory() throws HyracksDataException {
        outerBufferMngr.reset();
    }
}
