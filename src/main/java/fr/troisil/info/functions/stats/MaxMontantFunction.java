package fr.troisil.info.functions.stats;

import fr.troisil.info.functions.readers.DataAgricoleReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.function.Function;

public class MaxMontantFunction implements Function<Dataset<Row>, String> {

    private static SparkSession sparkSession;

    @Override
    public String apply(Dataset<Row> rowDataset) {


        Dataset<Row> ds = rowDataset.select("MONTANT TOTAL","LIBELLE DE LA COMMUNE DE RESIDENCE")
                .withColumn("MONTANT TOTAL",rowDataset.col("MONTANT TOTAL").cast("int"))
                .groupBy("LIBELLE DE LA COMMUNE DE RESIDENCE")
                .agg(functions.sum("MONTANT TOTAL").as("MONTANT TOTAL AIDES"));

        return ds.orderBy(functions.col("MONTANT TOTAL AIDES"))
                .select("LIBELLE DE LA COMMUNE DE RESIDENCE")
                .first()
                .getString(0);
    }


}
