package org.apache.flink.ml.recommendation.itemcf;

import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.param.Param;
import org.apache.flink.table.api.Table;

import java.io.IOException;
import java.util.Map;

public class ItemCfModel implements Model<ItemCfModel> {

    @Override
    public Table[] transform(Table... inputs) {
        return new Table[0];
    }

    @Override
    public void save(String path) throws IOException {

    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return null;
    }
}
