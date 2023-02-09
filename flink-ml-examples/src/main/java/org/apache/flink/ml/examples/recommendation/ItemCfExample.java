package org.apache.flink.ml.examples.recommendation;

import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ItemCfExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input data.
        Table inputTable =
                tEnv.fromDataStream(
                                env.fromElements(
                                        Row.of(0., 1),
                                        Row.of(2., 2),
                                        Row.of(1., 3),
                                        Row.of(1., 4),
                                        Row.of(0., 5),
                                        Row.of(2., 6),
                                        Row.of(1., 7),
                                        Row.of(1., 8),
                                        Row.of(2., 9),
                                        Row.of(0., 10),
                                        Row.of(0., 11),
                                        Row.of(1., 12),
                                        Row.of(1., 13)))
                        .as("user", "item");
        DataStream<Row> source = tEnv.toDataStream(inputTable);
        source.print();
        env.execute();
    }
}
