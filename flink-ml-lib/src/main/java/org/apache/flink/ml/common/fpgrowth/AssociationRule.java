package org.apache.flink.ml.common.fpgrowth;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.TreeMap;

public class AssociationRule {
    private static DataStream<Tuple4<int[], int[], Integer, double[]>> extractSingleConsequentRules(
            DataStream<Tuple2<Integer, int[]>> patterns,
            DataStream<Integer> transactionCount,
            DataStream<Tuple2<Integer, Integer>> itemCounts,
            final double minConfidence,
            final double minLift) {

        DataStream<Tuple5<int[], Integer, Integer, Integer, Boolean>> processedPatterns =
                patterns.flatMap(
                        new FlatMapFunction<Tuple2<Integer, int[]>,
                                Tuple5<int[], Integer, Integer, Integer, Boolean>>() {

                            @Override
                            public void flatMap(Tuple2<Integer, int[]> value,
                                                Collector<Tuple5<int[], Integer, Integer, Integer, Boolean>> out)
                                    throws Exception {
                                int[] items = value.f1;
                                out.collect(
                                        Tuple5.of(value.f1, value.f0, items[items.length - 1], items.length, false));
                                if (items.length <= 1) {
                                    return;
                                }
                                int tail = items[items.length - 1];
                                for (int i = items.length - 1; i >= 1; i--) {
                                    items[i] = items[i - 1];
                                }
                                items[0] = tail;
                                out.collect(Tuple5.of(items, value.f0, items[items.length - 1], items.length, true));
                            }
                        }).keyBy(t5 -> t5.f2);

        return null;
    }

    private static class ExtractSingleConsequentIterationBody implements IterationBody {
        private Map<int[], Integer> supportMap;
        Map<Integer, Integer> itemCounts;
        double transactionCount;
        final double minLift;
        final double minConfidence;

        ExtractSingleConsequentIterationBody(double minLift, double minConfidence) {
            this.minLift = minLift;
            this.minConfidence = minConfidence;
            supportMap = new TreeMap<>((o1, o2) -> {
                if (o1.length != o2.length) {
                    return Integer.compare(o1.length, o2.length);
                }
                for (int i = 0; i < o1.length; i++) {
                    if (o1[i] != o2[i]) {
                        return Integer.compare(o1[i], o2[i]);
                    }
                }
                return 0;
            });
        }

        @Override
        public IterationBodyResult process(DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Tuple5<int[], Integer, Integer, Integer, Boolean>> patterns = dataStreams.get(0);
            DataStream<Tuple4<int[], int[], Integer, double[]>> rules;

            rules = patterns.flatMap(
                    new FlatMapFunction<Tuple5<int[], Integer, Integer, Integer, Boolean>, Tuple4<int[], int[],
                            Integer, double[]>>() {
                        @Override
                        public void flatMap(
                                Tuple5<int[], Integer, Integer, Integer, Boolean> integerIntegerIntegerBooleanTuple5,
                                Collector<Tuple4<int[], int[], Integer, double[]>> collector) throws Exception {

                        }

                        private void exportRule(int[] x, int y, int suppXY,
                                                Collector<Tuple4<int[], int[], Integer, double[]>> collector) {
                            Integer suppX = supportMap.get(x);
                            Integer suppY = itemCounts.get(y);
                            assert suppX != null && suppY != null;
                            assert suppX >= suppXY && suppY >= suppXY;
                            double lift = suppXY * transactionCount / (suppX.doubleValue() * suppY.doubleValue());
                            double confidence = suppXY / suppX.doubleValue();
                            double support = suppXY / transactionCount;
                            if (lift >= minLift && confidence >= minConfidence) {
                                collector.collect(
                                        Tuple4.of(x, new int[] {y}, suppXY, new double[] {lift, support, confidence}));
                            }
                        }
                    });

            if (variableStreams.size() > 0) {
                rules = rules.union(variableStreams.get(0));
            }

            return null;
        }
    }
}
