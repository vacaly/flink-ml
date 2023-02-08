package org.apache.flink.ml.recommendation.itemcf;

import org.apache.flink.ml.common.param.HasDistanceMeasure;
import org.apache.flink.ml.common.param.HasOutputCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

public interface ItemCfParams<T> extends WithParams<T>, HasOutputCol<T>, HasDistanceMeasure<T> {
    Param<String> USER_COL =
            new StringParam("userCol", "Name of user column.", "user", ParamValidators.notNull());

    Param<String> ITEM_COL =
            new StringParam("itemCol", "Name of item column.", "item", ParamValidators.notNull());

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

}