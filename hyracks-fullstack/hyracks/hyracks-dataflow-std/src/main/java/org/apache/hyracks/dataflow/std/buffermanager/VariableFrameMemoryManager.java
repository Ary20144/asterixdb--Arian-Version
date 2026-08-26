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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

public class VariableFrameMemoryManager implements IFrameBufferManager {

    class PhysicalFrameOffset {
        ByteBuffer physicalFrame;
        int physicalOffset;

        PhysicalFrameOffset(ByteBuffer frame, int offset) {
            physicalFrame = frame;
            physicalOffset = offset;
        }

        void reset(ByteBuffer frame, int offset) {
            physicalFrame = frame;
            physicalOffset = offset;
        }
    }

    private final IFramePool framePool;
    private final IFrameFreeSlotPolicy freeSlotPolicy;
    private final List<PhysicalFrameOffset> physicalFrames = new ArrayList<>();
    private final List<BufferInfo> logicalFrames = new ArrayList<>();
    private int numPhysicalFrames = 0;
    private int numLogicalFrames = 0;

    //    //added for checkpoint 1 testing
    //    public volatile int takeRequestBytes = 32768;

    public VariableFrameMemoryManager(IFramePool framePool, IFrameFreeSlotPolicy freeSlotPolicy) {
        this.framePool = framePool;
        this.freeSlotPolicy = freeSlotPolicy;
    }

    private int findAvailableFrame(int frameSize) throws HyracksDataException {
        int frameId = freeSlotPolicy.popBestFit(frameSize);
        if (frameId >= 0) {
            return frameId;
        }
        ByteBuffer buffer = framePool.allocateFrame(frameSize);
        if (buffer != null) {
            IntSerDeUtils.putInt(buffer.array(), FrameHelper.getTupleCountOffset(buffer.capacity()), 0);
            if (numPhysicalFrames < physicalFrames.size()) {
                physicalFrames.get(numPhysicalFrames).reset(buffer, 0);
            } else {
                physicalFrames.add(new PhysicalFrameOffset(buffer, 0));
            }
            numPhysicalFrames++;
            return numPhysicalFrames - 1; // returns the index of the physical frame appended
        }
        return -1;
    }

    @Override
    public void reset() throws HyracksDataException {
        numPhysicalFrames = 0;
        numLogicalFrames = 0;
        freeSlotPolicy.reset();
        framePool.reset();
    }

    @Override
    public BufferInfo getFrame(int frameIndex, BufferInfo info) {
        if (frameIndex >= numLogicalFrames) {
            throw new IndexOutOfBoundsException();
        }
        info.reset(logicalFrames.get(frameIndex));
        return info;
    }

    @Override
    public int getNumFrames() {
        return numLogicalFrames;
    }

    @Override
    public int insertFrame(ByteBuffer frame) throws HyracksDataException {
        int frameSize = frame.capacity();
        //the following 4 lines have been added to take memory away mid copy
        //        int req = takeRequestBytes;
        //        int effectiveCap = framePool.getMemoryBudgetBytes() - (req > 0 ? req : 0);
        //        if ((numLogicalFrames * frameSize) + frameSize > effectiveCap) {
        //            return -1;
        //        }
        int physicalFrameId = findAvailableFrame(frameSize);
        if (physicalFrameId < 0) {
            return -1;
        }
        PhysicalFrameOffset frameOffset = physicalFrames.get(physicalFrameId);
        ByteBuffer buffer = frameOffset.physicalFrame;
        int offset = frameOffset.physicalOffset;
        System.arraycopy(frame.array(), 0, buffer.array(), offset, frameSize);
        if (offset + frameSize < buffer.capacity()) {
            freeSlotPolicy.pushNewFrame(physicalFrameId, buffer.capacity() - offset - frameSize);
        }
        frameOffset.physicalOffset = offset + frameSize;
        if (numLogicalFrames < logicalFrames.size()) {
            logicalFrames.get(numLogicalFrames).reset(buffer, offset, frameSize);
        } else {
            logicalFrames.add(new BufferInfo(buffer, offset, frameSize));
        }
        numLogicalFrames++;
        return numLogicalFrames - 1; // returns the index of the logical frame appended
    }

    @Override
    public void close() {
        numPhysicalFrames = 0;
        numLogicalFrames = 0;
        physicalFrames.clear();
        logicalFrames.clear();
        freeSlotPolicy.close();
        framePool.close();
    }

    /**
     * Bucketed release: detach the LAST n logical frames from this block and hand their
     * physical buffers to the caller (who has already spilled their contents and will free
     * them via the pool). Assumes logical frame == whole physical frame (uniform full-size
     * frames, as in NLJ where every inserted outer frame fills one pool frame — same
     * assumption as the CP3 spill path). Only call at scan time, when no inserts are in
     * flight; surviving frames keep their indices 0..getNumFrames()-1.
     */
    public int removeTrailingFrames(int n, List<ByteBuffer> removedBuffers) {
        int k = Math.min(n, numLogicalFrames);
        for (int i = 0; i < k; i++) {
            numLogicalFrames--;
            numPhysicalFrames--;
            removedBuffers.add(physicalFrames.get(numPhysicalFrames).physicalFrame);
        }
        return k;
    }
}
