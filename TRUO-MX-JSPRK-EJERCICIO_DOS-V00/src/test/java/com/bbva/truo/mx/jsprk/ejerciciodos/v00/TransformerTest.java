package com.bbva.truo.mx.jsprk.ejerciciodos.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.truo.mx.jsprk.ejerciciodos.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
               new StructField[]{
                         DataTypes.createStructField("CAMPO1", DataTypes.StringType, false),
                         DataTypes.createStructField("CAMPO2", DataTypes.StringType, false),
                         DataTypes.createStructField("CAMPO3", DataTypes.StringType, false),
               });
        Row firstRow = RowFactory.create("0182", "000001", "John Doe");
        Row secondRow = RowFactory.create("0182", "000002", "Mike Doe");
        Row thirdRow = RowFactory.create("0182", "000003", "Paul Doe");

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("sourceAlias1", dataset)));

        assertNotNull(datasetMap);

    }
    @Test
    void transform_Output1() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("ID_CAMPO", DataTypes.IntegerType, false),
                        DataTypes.createStructField("INE", DataTypes.IntegerType, false),
                        DataTypes.createStructField("INPUT_01", DataTypes.IntegerType, false),
                        DataTypes.createStructField("INPUT_02", DataTypes.IntegerType, false),
                        DataTypes.createStructField("INPUT_03", DataTypes.IntegerType, false),
                        DataTypes.createStructField("INPUT_04", DataTypes.IntegerType, false),
                });
        Row firstRow = RowFactory.create(4, 2547893, 30, 45, 30, 45);
        Row secondRow = RowFactory.create(5, 2123488, 20, 65, 20, 65);
        Row thirdRow = RowFactory.create(6, 7947820, 80, 32, 80, 32);

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("targetAlias2", dataset)));

        assertNotNull(datasetMap);
    }

}