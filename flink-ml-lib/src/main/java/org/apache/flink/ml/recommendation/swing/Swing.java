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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An AlgoOperator which implements the Swing algorithm.
 *
 * <p>Swing is an item recall algorithm. The topology of user-item graph usually can be described as
 * user-item-user or item-user-item, which are like 'swing'. For example, if both user <em>u</em>
 * and user <em>v</em> have purchased the same commodity <em>i</em> , they will form a relationship
 * diagram similar to a swing. If <em>u</em> and <em>v</em> have purchased commodity <em>j</em> in
 * addition to <em>i</em>, it is supposed <em>i</em> and <em>j</em> are similar. The similarity
 * between items in Swing is defined as
 *
 * <p>$$ w_{(i,j)}=\sum_{u\in U_i\cap U_j}\sum_{v\in U_i\cap
 * U_j}{\frac{1}{{(I_u+\alpha_1)}^\beta}}*{\frac{1}{{(I_v+\alpha_1)}^\beta}}*{\frac{1}{\alpha_2+|I_u\cap
 * I_v|}} $$
 *
 * <p>This implementation is based on the algorithm proposed in the paper: "Large Scale Product
 * Graph Construction for Recommendation in E-commerce" by Xiaoyong Yang, Yadong Zhu and Yi Zhang.
 * (<a href="https://arxiv.org/pdf/2010.05525.pdf">https://arxiv.org/pdf/2010.05525.pdf</a>)
 */
public class Swing implements AlgoOperator<Swing>, SwingParams<Swing> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public Swing() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {

        final String userCol = getUserCol();
        final String itemCol = getItemCol();
        Preconditions.checkArgument(inputs.length == 1);
        final ResolvedSchema schema = inputs[0].getResolvedSchema();

        if (!(Types.LONG.equals(TableUtils.getTypeInfoByName(schema, userCol))
                && Types.LONG.equals(TableUtils.getTypeInfoByName(schema, itemCol)))) {
            throw new IllegalArgumentException("The types of user and item columns must be Long.");
        }

        if (getMaxUserBehavior() < getMinUserBehavior()) {
            throw new IllegalArgumentException(
                    "The maxUserBehavior must be larger or equal to minUserBehavior.");
        }

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        SingleOutputStreamOperator<Tuple2<Long, Long>> itemUsers =
                tEnv.toDataStream(inputs[0])
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

        SingleOutputStreamOperator<Tuple3<Long, Long, Map<Long, String>>> userAllItemsStream =
                itemUsers
                        .keyBy(tuple -> tuple.f0)
                        .transform(
                                "collectingUserBehavior",
                                Types.TUPLE(
                                        Types.LONG,
                                        Types.LONG,
                                        Types.MAP(Types.LONG, Types.STRING)),
                                new CollectingUserBehavior(
                                        getMinUserBehavior(), getMaxUserBehavior()));

        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.LONG, Types.STRING},
                        new String[] {getItemCol(), getOutputCol()});

        DataStream<Row> output =
                userAllItemsStream
                        .keyBy(tuple -> tuple.f1)
                        .transform(
                                "computingSimilarItems",
                                outputTypeInfo,
                                new ComputingSimilarItems(
                                        getK(),
                                        getMaxUserNumPerItem(),
                                        getAlpha1(),
                                        getAlpha2(),
                                        getBeta()));

        return new Table[] {tEnv.fromDataStream(output)};
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static Swing load(StreamTableEnvironment tEnv, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    /**
     * Collects user behavior data and appends to the input table.
     *
     * <p>During the process, this operator collects users and all items he/she has purchased, and
     * its input table must be bounded. Because Flink doesn't support type info of `Set` officially,
     * The appended column is `Map` contains items as key and maps null value.
     */
    private static class CollectingUserBehavior
            extends AbstractStreamOperator<Tuple3<Long, Long, Map<Long, String>>>
            implements OneInputStreamOperator<
            Tuple2<Long, Long>, Tuple3<Long, Long, Map<Long, String>>>,
            BoundedOneInput {
        private final int minUserItemInteraction;
        private final int maxUserItemInteraction;

        // Maps a user id to a set of items. Because ListState cannot keep values of type `Set`,
        // we use `Map<Long, String>` with null values instead. So does `userItemsMap` and
        // `itemUsersMap` in `ComputingSimilarItems`.
        private Map<Long, Map<Long, String>> userItemsMap = new HashMap<>();

        private ListState<Map<Long, Map<Long, String>>> userAllItemsMapState;

        private CollectingUserBehavior(int minUserItemInteraction, int maxUserItemInteraction) {
            this.minUserItemInteraction = minUserItemInteraction;
            this.maxUserItemInteraction = maxUserItemInteraction;
        }

        @Override
        public void endInput() {

            userItemsMap.forEach(
                    (user, items) -> {
                        if (items.size() >= minUserItemInteraction
                                && items.size() <= maxUserItemInteraction) {
                            items.forEach(
                                    (item, nullValue) ->
                                            output.collect(
                                                    new StreamRecord<>(
                                                            new Tuple3<>(user, item, items))));
                        }
                    });

            userAllItemsMapState.clear();
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Long, Long>> element) {
            Tuple2<Long, Long> userAndItem = element.getValue();
            long user = userAndItem.f0;
            long item = userAndItem.f1;
            Map<Long, String> items = userItemsMap.get(user);

            if (items == null) {
                items = new LinkedHashMap<>();
            }

            if (items.size() <= maxUserItemInteraction) {
                items.put(item, null);
            }

            userItemsMap.put(user, items);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            userAllItemsMapState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "userAllItemsMapState",
                                            Types.MAP(
                                                    Types.LONG,
                                                    Types.MAP(Types.LONG, Types.STRING))));

            OperatorStateUtils.getUniqueElement(userAllItemsMapState, "userAllItemsMapState")
                    .ifPresent(
                            stat -> {
                                userItemsMap = stat;
                            });
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            userAllItemsMapState.update(Collections.singletonList(userItemsMap));
        }
    }

    /** Calculates similarity between items and keep top k similar items of each target item. */
    private static class ComputingSimilarItems extends AbstractStreamOperator<Row>
            implements OneInputStreamOperator<Tuple3<Long, Long, Map<Long, String>>, Row>,
            BoundedOneInput {

        private Map<Long, Map<Long, String>> userItemsMap = new HashMap<>();
        private Map<Long, Map<Long, String>> itemUsersMap = new HashMap<>();
        private ListState<Map<Long, Map<Long, String>>> userLocalItemsMapState;
        private ListState<Map<Long, Map<Long, String>>> itemUsersMapState;

        private final int k;
        private final int maxUserNumPerItem;
        private final int alpha1;
        private final int alpha2;
        private final double beta;

        private static final Character commaDelimiter = ',';
        private static final Character semicolonDelimiter = ';';

        private ComputingSimilarItems(
                int k, int maxUserNumPerItem, int alpha1, int alpha2, double beta) {
            this.k = k;
            this.maxUserNumPerItem = maxUserNumPerItem;
            this.alpha1 = alpha1;
            this.alpha2 = alpha2;
            this.beta = beta;
        }

        @Override
        public void endInput() throws Exception {

            Map<Long, Double> userWeights = new HashMap<>(userItemsMap.size());
            userItemsMap.forEach(
                    (k, v) -> {
                        int count = v.size();
                        userWeights.put(k, calculateWeight(count));
                    });

            for (long mainItem : itemUsersMap.keySet()) {
                List<Long> userList = sampleUserList(itemUsersMap.get(mainItem), maxUserNumPerItem);
                HashMap<Long, Double> id2swing = new HashMap<>();

                for (int i = 0; i < userList.size(); i++) {
                    long u = userList.get(i);
                    for (int j = i + 1; j < userList.size(); j++) {
                        long v = userList.get(j);
                        HashSet<Long> interaction = new HashSet<>(userItemsMap.get(u).keySet());
                        interaction.retainAll(userItemsMap.get(v).keySet());
                        if (interaction.size() == 0) {
                            continue;
                        }
                        double similarity =
                                (userWeights.get(u)
                                        * userWeights.get(v)
                                        / (alpha2 + interaction.size()));
                        for (long simItem : interaction) {
                            if (simItem == mainItem) {
                                continue;
                            }
                            double itemSimilarity =
                                    id2swing.getOrDefault(simItem, 0.0) + similarity;
                            id2swing.putIfAbsent(simItem, itemSimilarity);
                        }
                    }
                }

                ArrayList<Tuple2<Long, Double>> itemAndScore = new ArrayList<>();
                id2swing.forEach((key, value) -> itemAndScore.add(Tuple2.of(key, value)));

                itemAndScore.sort((o1, o2) -> Double.compare(o2.f1, o1.f1));

                if (itemAndScore.size() == 0) {
                    continue;
                }

                int itemNums = Math.min(k, itemAndScore.size());
                String itemList =
                        itemAndScore.stream()
                                .sequential()
                                .limit(itemNums)
                                .map(tuple2 -> "" + tuple2.f0 + commaDelimiter + tuple2.f1)
                                .collect(Collectors.joining("" + semicolonDelimiter));
                output.collect(new StreamRecord<>(Row.of(mainItem, itemList)));
            }

            userLocalItemsMapState.clear();
            itemUsersMapState.clear();
        }

        private double calculateWeight(int size) {
            return (1.0 / Math.pow(alpha1 + size, beta));
        }

        private static List<Long> sampleUserList(Map<Long, String> allUsers, int sampleSize) {
            int totalSize = allUsers.size();
            List<Long> userList = new ArrayList<>(allUsers.keySet());

            if (totalSize < sampleSize) {
                return userList;
            }

            Collections.shuffle(userList);
            return userList.subList(0, sampleSize);
        }

        @Override
        public void processElement(StreamRecord<Tuple3<Long, Long, Map<Long, String>>> streamRecord)
                throws Exception {
            Tuple3<Long, Long, Map<Long, String>> tuple3 = streamRecord.getValue();
            long user = tuple3.f0;
            long mainItem = tuple3.f1;
            Map<Long, String> items = tuple3.f2;

            userItemsMap.putIfAbsent(user, items);
            itemUsersMap.putIfAbsent(mainItem, new HashMap<>());
            itemUsersMap.get(mainItem).putIfAbsent(user, null);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            userLocalItemsMapState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "userLocalItemsMapState",
                                            Types.MAP(
                                                    Types.LONG,
                                                    Types.MAP(Types.LONG, Types.STRING))));

            OperatorStateUtils.getUniqueElement(userLocalItemsMapState, "userLocalItemsMapState")
                    .ifPresent(
                            stat -> {
                                userItemsMap = stat;
                            });

            itemUsersMapState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "itemUsersMapState",
                                            Types.MAP(
                                                    Types.LONG,
                                                    Types.MAP(Types.LONG, Types.STRING))));

            OperatorStateUtils.getUniqueElement(itemUsersMapState, "itemUsersMapState")
                    .ifPresent(
                            stat -> {
                                itemUsersMap = stat;
                            });
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            userLocalItemsMapState.update(Collections.singletonList(userItemsMap));
            itemUsersMapState.update(Collections.singletonList(itemUsersMap));
        }
    }
}
