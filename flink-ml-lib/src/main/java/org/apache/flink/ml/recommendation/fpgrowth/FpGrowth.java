package org.apache.flink.ml.recommendation.fpgrowth;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.Transformer;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.fpgrowth.FpTree;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.recommendation.swing.SwingParams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

public class FpGrowth implements Transformer<FpGrowth>, SwingParams<FpGrowth> {
    private static final String COUNT_ITEM = "COUNT_ITEM";
    private static final String PARTITION_ID = "PARTITION_ID";
    private static final String QUALIFIED_ITEMS = "QUALIFIED_ITEMS";
    public static final String ITEM_SEPARATOR = ",";

    @Override
    public Table[] transform(Table... inputs) {
        Table data = inputs[0];
        final String itemColName = getItemCol();
        final String userColName = getUserCol();
        final int minSupportThreshold = getMinUserBehavior();
        StreamTableEnvironment tenv =
                (StreamTableEnvironment) ((TableImpl) data).getTableEnvironment();

        DataStream<Set<String>> itemSets = tenv.toDataStream(data)
                .map(new MapFunction<Row, Set<String>>() {
                    private static final long serialVersionUID = -2980403044705096669L;

                    @Override
                    public Set<String> map(Row value) throws Exception {
                        Set<String> itemset = new HashSet<>();
                        String itemsetStr = (String) value.getField(itemColName);
                        if (!StringUtils.isNullOrWhitespaceOnly(itemsetStr)) {
                            String[] splited = itemsetStr.split(ITEM_SEPARATOR);
                            itemset.addAll(Arrays.asList(splited));
                        }
                        return itemset;
                    }
                });

        DataStream<String> items = itemSets.flatMap(new FlatMapFunction<Set<String>, String>() {
            @Override
            public void flatMap(Set<String> strings, Collector<String> collector) throws Exception {
                for (String s : strings) {
                    collector.collect(s);
                }
            }
        });

        // Count the support of each item.
        DataStream<Tuple2<String, Integer>> itemCounts = DataStreamUtils.keyedAggregate(
                items.keyBy(s -> s),
                new AggregateFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of("", 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(String itemId,
                                                       Tuple2<String, Integer> tuple2) {
                        tuple2.f0 = itemId;
                        tuple2.f1 = 1;
                        return tuple2;
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> tuple2) {
                        return tuple2;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(
                            Tuple2<String, Integer> tuple2,
                            Tuple2<String, Integer> acc1) {
                        acc1.f1 = tuple2.f1 + acc1.f1;
                        return acc1;
                    }
                },
                Types.TUPLE(Types.STRING, Types.INT),
                Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> qualifiedItems = itemCounts.filter(
                new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> t2) throws Exception {
                        return t2.f1 > minSupportThreshold;
                    }
                });

        DataStream<Row> dummyStream = tenv.toDataStream(tenv.fromValues(0));
        Map<String, DataStream<?>> broadcastMap = new HashMap<>(1);
        broadcastMap.put(QUALIFIED_ITEMS, qualifiedItems);

        DataStream<Tuple3<String, Integer, Integer>> itemCountIndex = BroadcastUtils.withBroadcastStream(
                Collections.singletonList(dummyStream),
                broadcastMap,
                inputList -> {
                    DataStream input = inputList.get(0);
                    return input.flatMap(
                            new RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                                List<Tuple2<String, Integer>> supportCount = getRuntimeContext().getBroadcastVariable(
                                        QUALIFIED_ITEMS);

                                @Override
                                public void flatMap(Tuple2<String, Integer> o,
                                                    Collector<Tuple3<String, Integer, Integer>> collector)
                                        throws Exception {
                                    if (null == supportCount) {
                                        supportCount = getRuntimeContext().getBroadcastVariable(QUALIFIED_ITEMS);
                                    }
                                    Integer[] order = new Integer[supportCount.size()];
                                    for (int i = 0; i < order.length; i++) {
                                        order[i] = i;
                                    }
                                    Arrays.sort(order, new Comparator<Integer>() {
                                        @Override
                                        public int compare(Integer o1, Integer o2) {
                                            Integer cnt1 = supportCount.get(o1).f1;
                                            Integer cnt2 = supportCount.get(o2).f1;
                                            if (cnt1.equals(cnt2)) {
                                                return supportCount.get(o1).f0.compareTo(supportCount.get(o2).f0);
                                            }
                                            return Integer.compare(cnt2, cnt1);
                                        }
                                    });
                                    for (int i = 0; i < order.length; i++) {
                                        collector.collect(Tuple3.of(supportCount.get(order[i]).f0,
                                                supportCount.get(order[i]).f1, i));
                                    }
                                }
                            });
                });

        DataStream<Tuple2<Integer, Integer>> itemPartition = partitionItems(itemCountIndex);
        DataStream<Tuple2<String, Integer>> itemIndex = itemCountIndex.map(t3 -> Tuple2.of(t3.f0, t3.f2));
        DataStream<int[]> transactions = itemTokenToIndex(itemSets, itemIndex);
        DataStream<Tuple2<Integer, int[]>> transactionGroups = genCondTransactions(transactions, itemPartition);
        DataStream<Tuple2<Integer, int[]>> indexPatterns = mineFreqPattern(transactionGroups, itemPartition);
        DataStream<Tuple2<String, String[]>> tokenPatterns = itemIndexToToken(indexPatterns, itemIndex);
        DataStream<Tuple4<int[], int[], Integer, double[]>> rules;


        return new Table[0];
    }

    private static DataStream<Tuple2<Integer, Integer>> partitionItems(
            DataStream<Tuple3<String, Integer, Integer>> itemCountIndex) {
        DataStream<Tuple2<Integer, Integer>> partition = itemCountIndex
                .transform(
                        "ComputingPartitionCost",
                        Types.TUPLE(
                                Types.INT,
                                Types.INT),
                        new ComputingPartitionCost()
                ).setParallelism(1);
        return partition;
    }

    private static class ComputingPartitionCost
            extends AbstractStreamOperator<Tuple2<Integer, Integer>>
            implements OneInputStreamOperator<Tuple3<String, Integer, Integer>, Tuple2<Integer, Integer>>,
            BoundedOneInput {
        List<Tuple2<Integer, Integer>> itemCounts;

        private ComputingPartitionCost() {
            // queue of tuple: partition, count
            int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
            itemCounts = new ArrayList<>();
        }

        @Override
        public void endInput() throws Exception {
            itemCounts.sort((o1, o2) -> {
                int cmp = Long.compare(o2.f1, o1.f1);
                return cmp == 0 ? Integer.compare(o1.f0, o2.f0) : cmp;
            });
            int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();

            // queue of tuple: partition, count
            PriorityQueue<Tuple2<Integer, Double>> queue = new PriorityQueue<>(numPartitions,
                    Comparator.comparingDouble(o -> o.f1));

            for (int i = 0; i < numPartitions; i++) {
                queue.add(Tuple2.of(i, 0.0));
            }

            List<Double> scaledItemCount = new ArrayList<>(itemCounts.size());
            for (int i = 0; i < itemCounts.size(); i++) {
                Tuple2<Integer, Integer> item = itemCounts.get(i);
                double pos = (double) i / ((double) itemCounts.size());
                double score = pos * item.f1.doubleValue();
                scaledItemCount.add(score);
            }

            List<Integer> order = new ArrayList<>(itemCounts.size());
            for (int i = 0; i < itemCounts.size(); i++) {
                order.add(i);
            }

            order.sort((o1, o2) -> {
                double s1 = scaledItemCount.get(o1);
                double s2 = scaledItemCount.get(o2);
                return Double.compare(s2, s1);
            });

            // greedily assign partition number to each item
            for (int i = 0; i < itemCounts.size(); i++) {
                Tuple2<Integer, Integer> item = itemCounts.get(order.get(i));
                double score = scaledItemCount.get(order.get(i));
                Tuple2<Integer, Double> target = queue.poll();
                int targetPartition = target.f0;
                target.f1 += score;
                queue.add(target);
                output.collect(new StreamRecord<>(Tuple2.of(item.f0, targetPartition)));
            }
        }

        @Override
        public void processElement(StreamRecord<Tuple3<String, Integer, Integer>> streamRecord) throws Exception {
            Tuple3<String, Integer, Integer> t3 = streamRecord.getValue();
            itemCounts.add(Tuple2.of(t3.f1, t3.f2));
        }
    }

    private static DataStream<int[]> itemTokenToIndex(DataStream<Set<String>> itemSets,
                                                      DataStream<Tuple2<String, Integer>> itemIndex) {
        Map<String, DataStream<?>> broadcastMap = new HashMap<>(1);
        broadcastMap.put("ITEM_INDEX", itemIndex);
        return BroadcastUtils.withBroadcastStream(
                Collections.singletonList(itemSets),
                broadcastMap,
                inputList -> {
                    DataStream transactions = inputList.get(0);
                    return transactions.map(new RichMapFunction<Set<String>, int[]>() {
                        Map<String, Integer> tokenToId;

                        @Override
                        public int[] map(Set<String> transaction) throws Exception {
                            if (null == tokenToId) {
                                tokenToId = new HashMap<>();
                                List<Tuple2<String, Integer>> itemIndexList = getRuntimeContext().getBroadcastVariable(
                                        "ITEM_INDEX");
                                for (Tuple2<String, Integer> t2 : itemIndexList) {
                                    tokenToId.put(t2.f0, t2.f1);
                                }
                            }
                            int[] items = new int[transaction.size()];
                            int len = 0;
                            for (String item : transaction) {
                                Integer id = tokenToId.get(item);
                                if (id != null) {
                                    items[len++] = id;
                                }
                            }
                            if (len > 0) {
                                int[] qualified = Arrays.copyOfRange(items, 0, len);
                                Arrays.sort(qualified);
                                return qualified;
                            } else {
                                return new int[0];
                            }
                        }
                    });
                }
        );
    }

    private static DataStream<Tuple2<Integer, int[]>> genCondTransactions(DataStream<int[]> transactions,
                                                                          DataStream<Tuple2<Integer, Integer>> targetPartition) {
        Map<String, DataStream<?>> broadcastMap = new HashMap<>(1);
        broadcastMap.put("partitioner", targetPartition);
        return transactions.flatMap(new RichFlatMapFunction<int[], Tuple2<Integer, int[]>>() {
            transient Map<Integer, Integer> partitioner;
            transient int[] flags;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                int numPartition = getRuntimeContext().getNumberOfParallelSubtasks();
                this.flags = new int[numPartition];
                List<Tuple2<Integer, Integer>> bc = getRuntimeContext()
                        .getBroadcastVariable("partitioner");
                partitioner = new HashMap<>();
                for (Tuple2<Integer, Integer> t2 : bc) {
                    partitioner.put(t2.f0, t2.f1);
                }
            }

            @Override
            public void flatMap(int[] transaction, Collector<Tuple2<Integer, int[]>> out) throws Exception {
                Arrays.fill(flags, 0);
                int cnt = transaction.length;
                for (int i = 0; i < cnt; i++) {
                    int lastPos = cnt - i;
                    int partition = this.partitioner.get(transaction[lastPos - 1]);
                    if (flags[partition] == 0) {
                        List<Integer> condTransaction = new ArrayList<>(lastPos);
                        for (int j = 0; j < lastPos; j++) {
                            condTransaction.add(transaction[j]);
                        }
                        int[] tr = new int[condTransaction.size()];
                        for (int j = 0; j < tr.length; j++) {
                            tr[j] = condTransaction.get(j);
                        }
                        out.collect(Tuple2.of(partition, tr));
                        flags[partition] = 1;
                    }
                }
            }

        });
    }

    private static DataStream<Tuple2<Integer, int[]>> mineFreqPattern(
            DataStream<Tuple2<Integer, int[]>> condTransactions,
            DataStream<Tuple2<Integer, Integer>> partitioner) {
        condTransactions = condTransactions
                .partitionCustom((chunkId, numPartitions) -> chunkId, x -> x.f0);
        partitioner = partitioner.partitionCustom((chunkId, numPartitions) -> chunkId, x -> x.f0);
        return condTransactions.connect(partitioner).transform(
                "mine-freq-pattern",
                Types.TUPLE(Types.INT, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO),
                new FpTreeConstructor()
        );
    }

    private static class FpTreeConstructor extends AbstractStreamOperator<Tuple2<Integer, int[]>>
            implements TwoInputStreamOperator<Tuple2<Integer, int[]>, Tuple2<Integer, Integer>, Tuple2<Integer, int[]>>,
            BoundedOneInput {

        private final FpTree tree = new FpTree();
        private List<Integer> itemList = new ArrayList<>();

        @Override
        public void open() throws Exception {
            super.open();
            tree.createTree();
        }

        @Override
        public void endInput() throws Exception {
            int minSupportCnt = 0;
            int maxLength = 100;
            tree.initialize();
            tree.printProfile();
            int[] suffices = new int[itemList.size()];
            for (int i = 0; i < suffices.length; i++) {
                suffices[i] = itemList.get(i);
            }
            tree.extractAll(suffices, minSupportCnt, maxLength, output);
            tree.destroyTree();
        }

        @Override
        public void processElement1(StreamRecord<Tuple2<Integer, int[]>> streamRecord) throws Exception {
            tree.addTransaction(streamRecord.getValue().f1);
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<Integer, Integer>> streamRecord) throws Exception {
            itemList.add(streamRecord.getValue().f1);
        }
    }

    private static DataStream<Tuple2<String, String[]>> itemIndexToToken(DataStream<Tuple2<Integer, int[]>> patterns,
                                                                         DataStream<Tuple2<String, Integer>> itemIndex) {

        Map<String, DataStream<?>> broadcastMap = new HashMap<>(1);
        broadcastMap.put("ITEM_INDEX", itemIndex);
        return BroadcastUtils.withBroadcastStream(
                Collections.singletonList(patterns),
                broadcastMap,
                inputList -> {
                    DataStream freqPatterns = inputList.get(0);
                    return freqPatterns.map(new RichMapFunction<Tuple2<Integer, int[]>, Tuple2<String, String[]>>() {
                        Map<Integer, String> tokenToId;

                        @Override
                        public Tuple2<String, String[]> map(Tuple2<Integer, int[]> pattern) throws Exception {
                            if (null == tokenToId) {
                                tokenToId = new HashMap<>();
                                List<Tuple2<String, Integer>> itemIndexList = getRuntimeContext().getBroadcastVariable(
                                        "ITEM_INDEX");
                                for (Tuple2<String, Integer> t2 : itemIndexList) {
                                    tokenToId.put(t2.f1, t2.f0);
                                }
                            }
                            String[] items = new String[pattern.f1.length];
                            int len = 0;
                            for (int item : pattern.f1) {
                                String token = tokenToId.get(item);
                            }
                            return Tuple2.of(tokenToId.get(pattern.f0), items);
                        }
                    });
                }
        );
    }

    @Override
    public void save(String path) throws IOException {

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return null;
    }
}
