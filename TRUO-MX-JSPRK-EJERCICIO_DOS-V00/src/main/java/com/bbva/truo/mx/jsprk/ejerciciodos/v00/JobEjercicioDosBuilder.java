package com.bbva.truo.mx.jsprk.ejerciciodos.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;

@Builder
public class JobEjercicioDosBuilder extends RegisterSparkBuilder {

    @Override
    public SourcesList registerSources() {
        //EXAMPLE WITH A LOCAL SOURCE FILE
        return SourcesList.builder()
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias1")
                        .physicalName("input1.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM sourceAlias1")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias2")
                        .physicalName("libro1.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM sourceAlias2")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias3")
                        .physicalName("libro2.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM sourceAlias3")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        //IF YOU WANT TRANSFORM CLASS
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
        //IF YOU WANT SQL TRANSFORM
        //return TransformConfig.SqlStatements.builder().addSql("targetAlias1", "SELECT CAMPO1 FROM sourceAlias1").build();
        //IF YOU DO NOT WANT TRANSFORM
        //return null;
    }

    @Override
    public TargetsList registerTargets() {
        //EXAMPLE WITH A LOCAL TARGET FILE
        return TargetsList.builder()
                .add(Target.File.Csv.builder()
                        .alias("targetAlias1")
                        .physicalName("output/output.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Target.File.Csv.builder()
                        .alias("targetAlias2")
                        .physicalName("output/output2.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

}