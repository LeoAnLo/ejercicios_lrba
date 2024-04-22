package com.bbva.indr.mx.jsprk.repopruebas.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> filteredDataset = dataset.filter(dataset.col("CAMPO2").equalTo("000002"));

        datasetsToWrite.put("targetAlias1", filteredDataset);

        return datasetsToWrite;
    }

}