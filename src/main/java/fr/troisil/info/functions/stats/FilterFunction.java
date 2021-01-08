package fr.troisil.info.functions.stats;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.function.Function;

public class FilterFunction implements Function<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {



        //FlatMapFunction<String,String> function = new CustomFlatMapFunction()::apply;

        //return rowDataset.flatMap(function, Encoders.STRING());


        Dataset<Row> filter = rowDataset.select("Montant total","Libelle de la commune de residence")
                .withColumn("Montant total",rowDataset.col("Montant Total").cast("int"))
                .groupBy("Libelle de la commune de residence")
                .agg(functions.sum("Montant Total").as("Montant total des aides"));



        return filter.orderBy(functions.col("Montant total des aides").desc_nulls_last());


    }
}
