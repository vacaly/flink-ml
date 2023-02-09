package org.apache.flink.ml.recommendation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.Preconditions;

public class RecommendationUtils {
    public static DataStream<Tuple2<Long, Long>> getUserItemPairStream(Table[] inputs, String userCol, String itemCol) {
        Preconditions.checkArgument(inputs.length == 1);
        final ResolvedSchema schema = inputs[0].getResolvedSchema();

        if (!(Types.LONG.equals(TableUtils.getTypeInfoByName(schema, userCol))
                && Types.LONG.equals(TableUtils.getTypeInfoByName(schema, itemCol)))) {
            throw new IllegalArgumentException("The types of user and item columns must be Long.");
        }

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        return tEnv.toDataStream(inputs[0])
                .map(
                        row -> {
                            if (row.getFieldAs(userCol) == null
                                    || row.getFieldAs(itemCol) == null) {
                                throw new RuntimeException(
                                        "Data of user and item column must not be null.");
                            }
                            return Tuple2.of(
                                    (Long) row.getFieldAs(userCol),
                                    (Long) row.getFieldAs(itemCol));
                        })
                .returns(Types.TUPLE(Types.LONG, Types.LONG));
    }
}
