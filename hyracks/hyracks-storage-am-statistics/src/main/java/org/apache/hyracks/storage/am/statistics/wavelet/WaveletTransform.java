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

package org.apache.hyracks.storage.am.statistics.wavelet;

import java.util.Stack;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IOrdinalPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISynopsis;
import org.apache.hyracks.storage.am.common.api.ISynopsisBuilder;
import org.apache.hyracks.storage.am.common.api.ISynopsisElement;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.statistics.common.AbstractSynopsisBuilder;
import org.apache.hyracks.storage.am.statistics.common.StatisticsCollector;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.util.objectpool.IObjectFactory;
import org.apache.hyracks.util.objectpool.MapObjectPool;

public class WaveletTransform extends StatisticsCollector {

    public WaveletTransform(IBufferCache bufferCache, IFileMapProvider fileMapProvider, FileReference file,
            int[] fields, int size, ITypeTraits[] fieldTypeTraits, IOrdinalPrimitiveValueProvider fieldValueProvider)
                    throws HyracksDataException {
        super(bufferCache, fileMapProvider, file, fields, size, fieldTypeTraits, fieldValueProvider);
    }

    @Override
    public ISynopsisBuilder createSynopsisBuilder(long numElements) throws HyracksDataException {
        return new SparseWaveletTransformBuilder();
    }

    class SparseWaveletTransformBuilder extends AbstractSynopsisBuilder {
        private final Stack<WaveletCoefficient> avgStack;
        private MapObjectPool<WaveletCoefficient, Integer> avgStackObjectPool;
        private Long prevPosition;
        private final WaveletSynopsis synopsis;

        @Override
        public ISynopsis<? extends ISynopsisElement> getSynopsis() {
            return synopsis;
        }

        public SparseWaveletTransformBuilder() throws HyracksDataException {
            this.synopsis = new WaveletSynopsis(fieldTypeTrait, size);

            avgStack = new Stack<>();
            avgStackObjectPool = new MapObjectPool<WaveletCoefficient, Integer>();
            IObjectFactory<WaveletCoefficient, Integer> waveletFactory = new IObjectFactory<WaveletCoefficient, Integer>() {
                @Override
                public WaveletCoefficient create(Integer level) {
                    return new WaveletCoefficient(0.0, level, -1);
                }
            };
            for (int i = -1; i <= synopsis.getMaxLevel(); i++) {
                avgStackObjectPool.register(i, waveletFactory);
            }
            //add first dummy average
            WaveletCoefficient dummyCoeff = avgStackObjectPool.allocate(-1);
            dummyCoeff.setIndex(-1);
            avgStack.push(dummyCoeff);
            prevPosition = null;
        }

        // Appends value to the coefficient with given index. If such coefficient is not found creates a new coeff
        public void apendToElement(long index, double appendValue, int maxLevel) {
            // TODO: do something better than linear search
            for (WaveletCoefficient coeff : synopsis) {
                if (coeff.getKey() == index) {
                    coeff.setValue(coeff.getValue() + appendValue / WaveletCoefficient
                            .getNormalizationCoefficient(maxLevel, WaveletCoefficient.getLevel(index, maxLevel)));
                    return;
                }
            }
            synopsis.addElement(index, appendValue, maxLevel);
        }

        // Modifies the wavelet coefficients in case when tuple was already transformed
        private void modifyTuple(WaveletCoefficient topCoeff, double tupleValue) {
            // the tuple on the top is always right end of dyadic range
            long rightCoeffId = topCoeff.getKey();
            for (long i = topCoeff.getLevel(); i > 0; i--) {
                // update coefficients, corresponding to all subranges having current position as they right end
                apendToElement(rightCoeffId, (i == 0 ? 1 : -1) * tupleValue / (1l << i), synopsis.getMaxLevel());
                rightCoeffId = rightCoeffId << 1 | 1;
            }
            // put modified top coefficient back to the stack
            topCoeff.setValue(topCoeff.getValue() + tupleValue / (1l << topCoeff.getLevel()));
            avgStack.push(topCoeff);

        }

        // Returns the parent wavelet coefficient for a given coefficient in the transform tree
        private WaveletCoefficient moveLevelUp(WaveletCoefficient childCoeff) {
            WaveletCoefficient parentCoeff = avgStackObjectPool.allocate(childCoeff.getLevel() + 1);
            parentCoeff.setValue(childCoeff.getValue() / 2.0);
            parentCoeff.setIndex(childCoeff.getParentCoeffIndex(synopsis.getDomainStart(), synopsis.getMaxLevel()));
            return parentCoeff;
        }

        // Calculates the position of the next tuple (on level 0) after given wavelet coefficient
        private long getTransformPosition(WaveletCoefficient coeff) {
            if (coeff.getLevel() < 0) {
                return synopsis.getDomainStart();
            } else if (coeff.getLevel() == 0) {
                return coeff.getKey() + 1;
            } else {
                return ((((coeff.getKey() + 1) << (coeff.getLevel() - 1)) - (1l << (synopsis.getMaxLevel() - 1))) << 1)
                        + synopsis.getDomainStart();
            }
        }

        // Combines two coeffs on the same level by averaging them and producing next level coefficient
        private void average(WaveletCoefficient leftCoeff, WaveletCoefficient rightCoeff, long domainMin, int maxLevel,
                WaveletCoefficient avgCoeff) {
            //        assert (leftCoeff.getLevel() == rightCoeff.getLevel());
            long coeffIdx = leftCoeff.getParentCoeffIndex(domainMin, maxLevel);
            // put detail wavelet coefficient to the coefficient queue
            synopsis.addElement(coeffIdx, (leftCoeff.getValue() - rightCoeff.getValue()) / 2.0, maxLevel);
            avgCoeff.setIndex(coeffIdx);
            avgCoeff.setValue((leftCoeff.getValue() + rightCoeff.getValue()) / 2.0);
        }

        // Pushes given coefficient on the stack, possibly triggering domino effect
        private void pushToStack(WaveletCoefficient newCoeff) {
            // if the coefficient on the top of the stack has the same level as new coefficient, they should be combined
            while (!avgStack.isEmpty() && avgStack.peek().getLevel() == newCoeff.getLevel()) {
                WaveletCoefficient topCoeff = avgStack.pop();
                // Guard against dummy coefficients
                if (topCoeff.getLevel() >= 0) {
                    //allocate next level coefficient from objectPool
                    WaveletCoefficient avgCoeff = avgStackObjectPool.allocate(topCoeff.getLevel() + 1);
                    // combine newCoeff and topCoeff by averaging them. Result coeff's level is greater than parent's level by 1
                    average(topCoeff, newCoeff, synopsis.getDomainStart(), synopsis.getMaxLevel(), avgCoeff);
                    avgStackObjectPool.deallocate(topCoeff.getLevel(), topCoeff);
                    avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
                    newCoeff = avgCoeff;
                }
            }
            // Guard against dummy coefficients
            if (newCoeff.getLevel() >= 0) {
                avgStack.push(newCoeff);
            }
        }

        private void transformTuple(WaveletCoefficient topCoeff, long tuplePosition, double tupleValue) {
            // 1st part: Upward transform
            WaveletCoefficient newCoeff = moveLevelUp(topCoeff);
            // Move the current top coefficient 1 level up as far as possible (until it will cover current position)
            while (!newCoeff.covers(tuplePosition, synopsis.getMaxLevel(), synopsis.getDomainStart())
                    && (avgStack.size() > 0 ? avgStack.peek().getLevel() > (newCoeff.getLevel() - 1) : true)
                    && topCoeff.getLevel() >= 0) {
                synopsis.addElement(newCoeff.getKey(), ((topCoeff.getKey() & 0x01) == 0 ? 1 : -1) * newCoeff.getValue(),
                        synopsis.getMaxLevel());
                topCoeff = newCoeff;
                newCoeff = moveLevelUp(newCoeff);
            }
            avgStackObjectPool.deallocate(newCoeff.getLevel(), newCoeff);
            newCoeff = topCoeff;
            // put the top coefficient (possibly modified) back on to the stack
            pushToStack(newCoeff);

            // 2nd part: Downward transform
            if (avgStack.size() > 0) {
                newCoeff = avgStack.peek();
            }
            // calculate the tuple position, where the transform currently stopped
            long transformPosition = getTransformPosition(newCoeff);
            // put all the coefficients, corresponding to dyadic ranges between current tuple position & transformPosition on the stack
            computeDyadicSubranges(tuplePosition, transformPosition);
            // put the last coefficient, corresponding to current tuple position on to the stack
            newCoeff = avgStackObjectPool.allocate(0);
            newCoeff.setValue(tupleValue);
            newCoeff.setIndex(tuplePosition);
            pushToStack(newCoeff);
        }

        // Method calculates decreasing level dyadic intervals between tuplePosition&currTransformPosition and saves corresponding coefficients in the avgStack
        private void computeDyadicSubranges(long tuplePosition, long currTransformPosition) {
            while (tuplePosition != currTransformPosition) {
                WaveletCoefficient coeff;
                if (avgStack.size() > 0) {
                    coeff = avgStackObjectPool.allocate(avgStack.peek().getLevel());
                    coeff.setValue(0.0);
                    // starting with the sibling of the top coefficient on the stack
                    coeff.setIndex(avgStack.peek().getKey() + 1l);
                }
                // special case when there is no coeffs on the stack.
                else {
                    coeff = avgStackObjectPool.allocate(synopsis.getMaxLevel());
                    coeff.setValue(0.0);
                    // Starting descent from top coefficient, i.e. the one with index == 1, level == maxLevel
                    coeff.setIndex(1l);
                }
                // decrease the coefficient level until it stops covering tuplePosition
                while (coeff.covers(tuplePosition, synopsis.getMaxLevel(), synopsis.getDomainStart())) {
                    avgStackObjectPool.deallocate(coeff.getLevel(), coeff);
                    WaveletCoefficient newCoeff = avgStackObjectPool.allocate(coeff.getLevel() - 1);
                    newCoeff.setValue(0.0);
                    if (newCoeff.getLevel() == 0) {
                        newCoeff.setIndex(((coeff.getKey() - (1l << (synopsis.getMaxLevel() - 1))) << 1)
                                + synopsis.getDomainStart());
                    } else {
                        newCoeff.setIndex(coeff.getKey() << 1);
                    }
                    coeff = newCoeff;
                }
                // we don't add newCoeff to the wavelet coefficient collection, since it's value is 0. Keep it only in average stack
                pushToStack(coeff);
                currTransformPosition = getTransformPosition(coeff);
            }
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {

            WaveletCoefficient topCoeff = avgStack.pop();
            boolean neg = false;
            if (isAntimatterTuple) {
                neg = ((ILSMTreeTupleReference) tuple).isAntimatter();
            }
            long currTuplePosition = fieldValueProvider.getOrdinalValue(tuple.getFieldData(field),
                    tuple.getFieldStart(field));
            double currTupleValue = neg ? -1.0 : 1.0;

            // check whether tuple with this position was already seen
            if (prevPosition != null && prevPosition.equals(currTuplePosition)) {
                modifyTuple(topCoeff, currTupleValue);
            } else {
                transformTuple(topCoeff, currTuplePosition, currTupleValue);
            }
            prevPosition = currTuplePosition;
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            WaveletCoefficient topCoeff = avgStack.pop();
            if (topCoeff.getKey() > 0) {
                if (prevPosition == null || prevPosition.equals(synopsis.getDomainEnd())) {
                    //complete transform by submitting dummy tuple with the last position available for given domain
                    transformTuple(topCoeff, synopsis.getDomainEnd(), 0.0);
                    topCoeff = avgStack.pop();
                }
                // now the transform is complete the top coefficient on the stack is global average, i.e. coefficient with index==0
                synopsis.addElement(0l, topCoeff.getValue(), synopsis.getMaxLevel());

                synopsis.sortOnKey();
                //TODO:for now disable local persistence of the stats
                //persistSynopsis(synopsis);
            }
        }

        @Override
        public void abort() throws HyracksDataException {
        }

    }

}
