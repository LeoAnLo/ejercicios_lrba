package com.bbva.truo.mx.jsprk.ejerciciodos.v00;

import com.bbva.lrba.spark.transformers.Transform;
import com.bbva.truo.mx.jsprk.ejerciciodos.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        SparkSession spark = SparkSession.builder()
                .appName("ejercicio_2")
                .master("local[*]")
                .getOrCreate();

//        Dataset<Row> dataset = datasetsFromRead.get("sourceAlias1").as(Encoders.bean(Row.class));

        Dataset<Row> ds = spark.read().option("header","True").option("delimiter",";").csv("local-execution/files/libro1.csv");
        Dataset<Row> ds_1 = spark.read().option("header","True").option("delimiter",";").csv("local-execution/files/libro2.csv");
        Dataset<Row> ds_ds1 = ds.alias("A").join(ds_1.alias("B"), ds.col("input_01").equalTo(ds_1.col("input_03")), "inner");
        Dataset<Row> ds_ds1_sinDNI = ds_ds1.drop("DNI");
        Dataset<Row> dataSet3 = ds_ds1_sinDNI.select("A.ID_CAMPO","B.INE","A.INPUT_01","A.INPUT_02","B.INPUT_03","B.INPUT_04");

        dataSet3.show();

        //datasetsToWrite.put("targetAlias1", dataset.toDF());
        datasetsToWrite.put("targetAlias2", dataSet3.toDF());

       // dataSet3.write().option("header", "true").option("delimiter", ";").csv("local-execution/files/salida");

        return datasetsToWrite;
    }

}