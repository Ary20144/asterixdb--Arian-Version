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
//to simulate broker
import java.util.Random;

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
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
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
    private int drainCount = 0;
    //broker simulation state
    private final long brokerSeed = System.nanoTime() ^ System.identityHashCode(this);
    private final Random brokerRnd = new Random(brokerSeed);
    private int victimCount = 0; // checkpoint-2 victimizations
    private int grantCount = 0;
    private int rejectCount = 0;
    private int originalBudgetBytes;// growth cap reference
    private int frameInterval = 100;
    private int frameCounter = 0;
    // BENCHMARK KNOBS: set manually per run config (see benchmark matrix), then rebuild. 0 = off.
    private int cp1VictimPercent = 0; // mid-fill victim chance, rolled every frameInterval frames
    private int cp2VictimPercent = 0; // overflow victim chance, rolled once per overflow
    private int grantPercent = 0; // grow-grant chance when not a victim at overflow
    private int grantCapBytes = 64 * 1024 * 1024; // grant ceiling; set to ~dataset size (0 = classic 4x original)
    // BENCHMARK METRICS (reported in SUMMARY)
    private long sFrameReads = 0; // S frames read back from the run file (disk contact incl. replay)
    private long matchCount = 0; // joined pairs appended (replay included) — the true result cardinality
    private long cp12RespNanosTotal = 0; // CP1/CP2 boundary releases: decision -> released
    private int cp12RespEvents = 0;
    private long cp3RespNanosTotal = 0; // CP3: spill decision -> released (includes block write)
    private int cp3RespEvents = 0;
    private long cp3RequestNanos = 0; // stamp of the most recent CP3 spill decision
    // CHECKPOINT 3: spill the pinned R block mid-S-scan so memory can be released immediately.
    // Multiple spill events per join (bounded by cp3MaxSpills); inner joins only
    // (LOJ needs outerMatchLOJ state spilled too — future work). Each spill parks one
    // (file, resumePoint) debt: block_k x S[j_k..end), repaid independently in completeJoin.
    private IHyracksJobletContext jobletContext; // promoted from ctor param (spill file creation)
    private final List<RunFileWriter> cp3SpillWriters = new ArrayList<>(); // parked debts
    private final List<Integer> cp3ResumePoints = new ArrayList<>(); // j_k per debt
    private int cp3MaxSpills = 10; // bounds deferred work + disk
    private int cp3ResumeInnerFrames = 0; // skip of the debt CURRENTLY replaying
    private boolean cp3SpillJustHappened = false; // signals the post-drain serve
    private boolean cp3Replaying = false; // skip-mode flag (replaces a skip parameter)
    private int cp3SpillCount = 0;
    private int cp3VictimPercent = 0; // mid-drain spill chance per inner-frame boundary (0 = off)
    // BUCKETED RELEASE: give back memory at frame granularity instead of all-or-half.
    // false = original behavior (idle-only takes; CP3 spills the WHOLE block and aborts the scan).
    // true  = releases also shrink unallocated headroom, and CP3 spills ONLY the frames needed
    //         (one bucket = one debt) while the scan continues on the surviving frames.
    private boolean bucketedRelease = true;
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
        //added to test cp3 behaviour
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
        this.originalBudgetBytes = outerBufferMngrMemBudgetInBytes;
        this.outerBufferMngr = new VariableFrameMemoryManager(framePool, FrameFreeSlotPolicyFactory
                .createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT, outerBufferMngrMemBudgetInFrames));

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
     added for testing behaviour of a randomly assigning broker
     */

    /** One coin flip: true with the given percent chance (0 = never, 100 = always). */
    private boolean coin(int percent) {
        return percent > 0 && brokerRnd.nextInt(100) < percent;
    }

    /** Victim action: release half of the OUTER (R) pool's current budget, if the pool keeps its floor.
     *  Response time is decision -> released: for CP1/CP2 the decision is this call; for a CP3 serve
     *  the decision was the spill (stamped in cp3RequestNanos), so the block write is part of the wait. */
    private void releaseHalfOfOuterBudget(boolean servingCP3) {
        long t0 = System.nanoTime();
        int current = framePool.getMemoryBudgetBytes();
        int half = current / 2;
        if (current - half >= MIN_POOL_FRAMES * framePool.getMinFrameSize()) {
            int got = framePool.takeUnusedMemory(half);
            int headroom = 0;
            if (bucketedRelease && got < half) {
                // bucketed tier 2: budget never allocated into frames costs nothing to surrender
                headroom = framePool.shrinkUnallocated(half - got);
                got += headroom;
            }
            victimCount++;
            if (servingCP3) {
                cp3RespNanosTotal += System.nanoTime() - cp3RequestNanos;
                cp3RespEvents++;
            } else {
                cp12RespNanosTotal += System.nanoTime() - t0;
                cp12RespEvents++;
            }
            LOGGER.info("NLJ-BROKER drain#{} VICTIM releasing half of outer budget: asked={} took={} (headroom={}) capNow={}",
                    drainCount, half, got, headroom, framePool.getMemoryBudgetBytes());
        } else {
            LOGGER.info("NLJ-BROKER drain#{} VICTIM-DECLINED cap={} at pool floor", drainCount, current);
        }
    }

    /** Grow request: double (capped at 4x original) or rejected. Returns whether granted. */
    private boolean askBrokerForMore() {
        int current = framePool.getMemoryBudgetBytes();
        int ceiling = grantCapBytes > 0 ? grantCapBytes : originalBudgetBytes * 4;
        if (coin(grantPercent) && current * 2 <= ceiling) {
            framePool.updateBudget(current * 2);
            grantCount++;
            LOGGER.info("NLJ-BROKER drain#{} GRANTED {} -> {}", drainCount, current, current * 2);
            return true;
        }
        rejectCount++;
        LOGGER.info("NLJ-BROKER drain#{} REJECTED cap stays {}", drainCount, current);
        return false;
    }

    //helper functions added for cp3
    private boolean shouldCP3Spill(int innerFramesProcessed) {
        int current = framePool.getMemoryBudgetBytes();
        return !isLeftOuter && !cp3Replaying && cp3SpillWriters.size() < cp3MaxSpills && innerFramesProcessed > 0
                && current - current / 2 >= MIN_POOL_FRAMES * framePool.getMinFrameSize() // don't spill for a take the floor would decline
                && coin(cp3VictimPercent);
    }

    /** Shared drain epilogue: count, reset, and serve any pending release (CP2 victim and/or CP3 spill). */
    private void resetAfterDrain(boolean releaseMemory) throws HyracksDataException {
        drainCount++;
        outerBufferMngr.reset();
        boolean servingCP3 = cp3SpillJustHappened;
        if (cp3SpillJustHappened) {
            cp3SpillJustHappened = false;
            releaseMemory = true; // CP3's immediate serve rides this path
        }
        if (releaseMemory) {
            releaseHalfOfOuterBudget(servingCP3);
        }
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        accessorOuter.reset(outerBuffer);
        if (accessorOuter.getTupleCount() <= 0) {
            return;
        }
        frameCounter++;
        // CHECKPOINT 1: R still filling — give-up side ONLY
        if (frameCounter % frameInterval == 0 && coin(cp1VictimPercent)) {
            releaseHalfOfOuterBudget(false);
        }
        if (outerBufferMngr.insertFrame(outerBuffer) < 0) {
            // CHECKPOINT 2: out of memory — victim check FIRST
            if (coin(cp2VictimPercent)) {
                //                    multiBlockJoin(writer);
                //                    drainCount++;
                //                    outerBufferMngr.reset();
                //                    releaseHalfOfOuterBudget();
                multiBlockJoin(writer);
                resetAfterDrain(true);
            } else {
                // not victim: ask for more BEFORE spilling — a grant makes the drain unnecessary
                if (askBrokerForMore() && outerBufferMngr.insertFrame(outerBuffer) >= 0) {
                    return; // grant absorbed the frame: S-scan avoided
                }
                //                    multiBlockJoin(writer);              // rejected -> spill as usual
                //                    drainCount++;
                //                    outerBufferMngr.reset();
                multiBlockJoin(writer);
                resetAfterDrain(false);
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

    //changed for testing cp3 behaviour
    private void multiBlockJoin(IFrameWriter writer) throws HyracksDataException {
        int outerBufferFrameCount = outerBufferMngr.getNumFrames();
        if (outerBufferFrameCount == 0) {
            return;
        }
        RunFileReader runFileReader = runFileWriter.createReader();
        int innerFramesProcessed = 0;
        try {
            runFileReader.open();
            if (isLeftOuter) {
                outerMatchLOJ.clear();
            }
            while (runFileReader.nextFrame(innerBuffer)) {
                sFrameReads++; // every S frame pulled from the run file, including replay-skipped ones
                if (cp3Replaying && innerFramesProcessed < cp3ResumeInnerFrames) {
                    // replay mode: these S frames were already joined against the spilled block
                    innerFramesProcessed++;
                    continue;
                }
                if (shouldCP3Spill(innerFramesProcessed)) {
                    int bucketFrames = (framePool.getMemoryBudgetBytes() / 2) / framePool.getMinFrameSize();
                    if (bucketedRelease && bucketFrames > 0 && bucketFrames < outerBufferFrameCount) {
                        // CHECKPOINT 3, bucketed: spill ONLY the requested frames (one bucket = one
                        // debt), free them right here, and keep scanning the surviving frames
                        outerBufferFrameCount =
                                spillBucketMidScan(bucketFrames, innerFramesProcessed, outerBufferFrameCount);
                    } else {
                        // CHECKPOINT 3, original: preserve the whole block, abort;
                        // the caller's resetAfterDrain() frees and releases immediately
                        spillCurrentOuterBlock(innerFramesProcessed);
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
                innerFramesProcessed++;
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

    /** Preserve the whole pinned block on disk so reset() can legally free it.
     *  Assumes logical frame == whole physical frame (uniform 32KB, verified in this setup).
     *  Each call parks one independent debt: this block x S[innerFramesProcessed..end). */
    private void spillCurrentOuterBlock(int innerFramesProcessed) throws HyracksDataException {
        cp3RequestNanos = System.nanoTime(); // broker "request" moment; released in resetAfterDrain
        FileReference file =
                jobletContext.createManagedWorkspaceFile("NLJCp3Spill" + cp3SpillWriters.size() + this.toString());
        RunFileWriter spillWriter = new RunFileWriter(file, jobletContext.getIoManager());
        spillWriter.open();
        int n = outerBufferMngr.getNumFrames();
        for (int i = 0; i < n; i++) {
            spillWriter.nextFrame(outerBufferMngr.getFrame(i, tempInfo).getBuffer());
        }
        spillWriter.close();
        cp3SpillWriters.add(spillWriter);
        cp3ResumePoints.add(innerFramesProcessed);
        cp3SpillJustHappened = true;
        cp3SpillCount++;
        LOGGER.info("NLJ-BROKER drain#{} CP3-SPILL #{} of max {}: {} frames after {} inner frames; rest deferred",
                drainCount, cp3SpillWriters.size(), cp3MaxSpills, n, innerFramesProcessed);
    }

    /** CHECKPOINT 3, bucketed (cut-to-order): spill only the LAST nFrames of the pinned block
     *  as one debt (this bucket x S[innerFramesProcessed..end)), free exactly those frames, and
     *  return the surviving frame count so the caller's scan continues on frames 0..survivors-1.
     *  Same 1:1 logical/physical frame assumption as spillCurrentOuterBlock. The release happens
     *  HERE (write + free), so no epilogue serve is needed and the scan is never aborted. */
    private int spillBucketMidScan(int nFrames, int innerFramesProcessed, int frameCount)
            throws HyracksDataException {
        cp3RequestNanos = System.nanoTime(); // broker "request" moment; released at end of this method
        FileReference file =
                jobletContext.createManagedWorkspaceFile("NLJCp3Bucket" + cp3SpillWriters.size() + this.toString());
        RunFileWriter spillWriter = new RunFileWriter(file, jobletContext.getIoManager());
        spillWriter.open();
        for (int i = frameCount - nFrames; i < frameCount; i++) {
            spillWriter.nextFrame(outerBufferMngr.getFrame(i, tempInfo).getBuffer());
        }
        spillWriter.close();
        cp3SpillWriters.add(spillWriter); // replayed by the existing replayCP3Spills, unchanged
        cp3ResumePoints.add(innerFramesProcessed);
        cp3SpillCount++;
        victimCount++;
        List<ByteBuffer> evicted = new ArrayList<>();
        outerBufferMngr.removeTrailingFrames(nFrames, evicted);
        int freed = 0;
        for (ByteBuffer b : evicted) {
            freed += framePool.releaseSpecificFrame(b); // cap + ledger updated per actual bytes
        }
        cp3RespNanosTotal += System.nanoTime() - cp3RequestNanos;
        cp3RespEvents++;
        LOGGER.info(
                "NLJ-BROKER drain#{} CP3-BUCKET #{} of max {}: spilled {} frames after {} inner frames, freed={} capNow={}; scan continues on {} frames",
                drainCount, cp3SpillWriters.size(), cp3MaxSpills, nFrames, innerFramesProcessed, freed,
                framePool.getMemoryBudgetBytes(), frameCount - nFrames);
        return frameCount - nFrames;
    }

    /** Repay every parked debt: spilled block_k x S[j_k..end), each via the normal insert/drain cycle. */
    private void replayCP3Spills(IFrameWriter writer) throws HyracksDataException {
        for (int k = 0; k < cp3SpillWriters.size(); k++) {
            cp3ResumeInnerFrames = cp3ResumePoints.get(k); // this debt's skip
            int replayed = 0;
            RunFileReader spillReader = cp3SpillWriters.get(k).createDeleteOnCloseReader();
            IFrame replayFrame = new VSizeFrame(jobletContext);
            cp3Replaying = true;
            try {
                outerBufferMngr.reset(); // THE FIX: pool may hold the fully-drained final block (or debt k-1)
                spillReader.open();
                try {
                    while (spillReader.nextFrame(replayFrame)) {
                        if (outerBufferMngr.insertFrame(replayFrame.getBuffer()) < 0) {
                            multiBlockJoin(writer);
                            resetAfterDrain(false); // honest drainCount, same epilogue
                            outerBufferMngr.insertFrame(replayFrame.getBuffer());
                        }
                        replayed++;
                    }
                } finally {
                    spillReader.close();
                }
                multiBlockJoin(writer); // last partial debt block
                resetAfterDrain(false);
            } finally {
                cp3Replaying = false;
            }
            LOGGER.info("NLJ-BROKER CP3-REPLAY {}/{} frames={} skippedInnerFrames={}", k + 1, cp3SpillWriters.size(),
                    replayed, cp3ResumeInnerFrames);
        }
        cp3SpillWriters.clear();
        cp3ResumePoints.clear();
        cp3ResumeInnerFrames = 0;
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
            if (cp3SpillJustHappened) { // spill fired during the FINAL drain
                resetAfterDrain(false); // flag inside makes it release
            }
            replayCP3Spills(writer);
        } finally {
            runFileWriter.eraseClosed();
        }
        appender.write(writer, true);
        LOGGER.info(
                "NLJ-BROKER SUMMARY seed={} knobs[cp1={} cp2={} grant={} cp3={} maxSpills={} bucketed={}] drains={} victims={} "
                        + "granted={} rejected={} cp3Spills={} sFrameReads={} matches={} "
                        + "outerFramesIn={} outerBytesIn={} innerRunFileBytes={} finalCap={} totalBytesGivenUp={} "
                        + "avgRespMicrosCp12={} (n={}) avgRespMicrosCp3={} (n={})",
                brokerSeed, cp1VictimPercent, cp2VictimPercent, grantPercent, cp3VictimPercent, cp3MaxSpills,
                bucketedRelease, drainCount, victimCount, grantCount, rejectCount, cp3SpillCount, sFrameReads,
                matchCount, frameCounter, (long) frameCounter * framePool.getMinFrameSize(),
                runFileWriter.getFileSize(),
                framePool.getMemoryBudgetBytes(), framePool.getGivenBytes(),
                cp12RespNanosTotal / Math.max(cp12RespEvents, 1) / 1000, cp12RespEvents,
                cp3RespNanosTotal / Math.max(cp3RespEvents, 1) / 1000, cp3RespEvents);

    }

    public void releaseMemory() throws HyracksDataException {
        outerBufferMngr.reset();
    }
}
