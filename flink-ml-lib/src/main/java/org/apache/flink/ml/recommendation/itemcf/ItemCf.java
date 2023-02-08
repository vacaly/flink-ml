package org.apache.flink.ml.recommendation.itemcf;

import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.table.api.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ItemCf  implements Estimator<ItemCf, ItemCfModel> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private Table initModelDataTable;

    public ItemCf() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public ItemCfModel fit(Table... inputs) {
        return null;
    }

    @Override
    public void save(String path) throws IOException {
        
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return null;
    }
}
