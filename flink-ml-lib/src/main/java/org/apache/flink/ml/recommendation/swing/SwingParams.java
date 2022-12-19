/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.recommendation.swing;

import org.apache.flink.ml.common.param.HasOutputCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

/**
 * Params for {@link Swing}.
 *
 * @param <T> The class type of this instance.
 */
public interface SwingParams<T> extends WithParams<T>, HasOutputCol<T> {
    Param<String> USER_COL =
            new StringParam("userCol", "Name of user column.", "user", ParamValidators.notNull());

    Param<String> ITEM_COL =
            new StringParam("itemCol", "Name of item column.", "item", ParamValidators.notNull());

    Param<Integer> MAX_USER_NUM_PER_ITEM =
            new IntParam(
                    "maxUserNumPerItem",
                    "The max number of users that has purchased for each item. If the number of users that have "
                            + "purchased this item is larger than this value, then only maxUserNumPerItem users will "
                            + "be sampled and used in the computation logic.",
                    1000,
                    ParamValidators.gt(0));

    Param<Integer> K =
            new IntParam(
                    "k",
                    "The max number of similar items to output for each item.",
                    100,
                    ParamValidators.gt(0));

    Param<Integer> MIN_USER_BEHAVIOR =
            new IntParam(
                    "minUserBehavior",
                    "The min number of interaction behavior between item and user.",
                    10,
                    ParamValidators.gt(0));

    Param<Integer> MAX_USER_BEHAVIOR =
            new IntParam(
                    "maxUserBehavior",
                    "The max number of interaction behavior between item and user. "
                            + "The algorithm filters out activate users.",
                    1000,
                    ParamValidators.gt(0));

    Param<Integer> ALPHA1 =
            new IntParam(
                    "alpha1",
                    "This parameter is used to calculate weight of each user. "
                            + "The higher alpha1 is, the smaller weight each user gets.",
                    15,
                    ParamValidators.gtEq(0));

    Param<Integer> ALPHA2 =
            new IntParam(
                    "alpha2",
                    "This parameter is used to calculate similarity of users. "
                            + "The higher alpha2 is, the less the similarity score is.",
                    0,
                    ParamValidators.gtEq(0));

    Param<Double> BETA =
            new DoubleParam(
                    "beta",
                    "This parameter is used to calculate weight of each user. "
                            + "The higher beta is, the weight is subject to exponential decay.",
                    0.3,
                    ParamValidators.gtEq(0));

    default String getUserCol() {
        return get(USER_COL);
    }

    default T setUserCol(String value) {
        return set(USER_COL, value);
    }

    default String getItemCol() {
        return get(ITEM_COL);
    }

    default T setItemCol(String value) {
        return set(ITEM_COL, value);
    }

    default int getK() {
        return get(K);
    }

    default T setK(Integer value) {
        return set(K, value);
    }

    default int getMaxUserNumPerItem() {
        return get(MAX_USER_NUM_PER_ITEM);
    }

    default T setMaxUserNumPerItem(Integer value) {
        return set(MAX_USER_NUM_PER_ITEM, value);
    }

    default int getMinUserBehavior() {
        return get(MIN_USER_BEHAVIOR);
    }

    default T setMinUserBehavior(Integer value) {
        return set(MIN_USER_BEHAVIOR, value);
    }

    default int getMaxUserBehavior() {
        return get(MAX_USER_BEHAVIOR);
    }

    default T setMaxUserBehavior(Integer value) {
        return set(MAX_USER_BEHAVIOR, value);
    }

    default int getAlpha1() {
        return get(ALPHA1);
    }

    default T setAlpha1(Integer value) {
        return set(ALPHA1, value);
    }

    default int getAlpha2() {
        return get(ALPHA2);
    }

    default T setAlpha2(Integer value) {
        return set(ALPHA2, value);
    }

    default double getBeta() {
        return get(BETA);
    }

    default T setBeta(Double value) {
        return set(BETA, value);
    }
}
