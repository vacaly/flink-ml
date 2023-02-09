package org.apache.flink.ml.recommendation.itemcf;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.recommendation.RecommendationUtils;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ItemCf implements Estimator<ItemCf, ItemCfModel>, ItemCfParams<ItemCf> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table initModelDataTable;

    public ItemCf() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public ItemCfModel fit(Table... inputs) {

        final String userCol = getUserCol();
        final String itemCol = getItemCol();
        DataStream<Tuple2<Long, Long>> itemUserPairStream = RecommendationUtils
                .getUserItemPairStream(inputs, userCol, itemCol);

        SingleOutputStreamOperator<Integer> similaritySumSet = itemUserPairStream
                .keyBy(itemCol)
                .transform(
                        "itemPartitioner",
                        Types.INT,
                        new ItemPartitioner(itemCol));
        return null;
    }

    private static class ItemPartitioner extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Tuple2<Long, Long>, Integer>, BoundedOneInput {
        HashSet<Long> intSet = new HashSet<>();
        String itemCol;

        private ItemPartitioner(String itemCol) {
            this.itemCol = itemCol;
        }

        @Override
        public void endInput() {
            output.collect(new StreamRecord<>(intSet.size()));
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
            intSet.add(element.getValue().f1);
        }
    }

    @Override
    public void save(String path) throws IOException {

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return null;
    }
}
