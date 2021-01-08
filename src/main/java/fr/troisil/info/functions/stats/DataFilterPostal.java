package fr.troisil.info.functions.stats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.function.Function;

public class DataFilterPostal implements Function<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {



        return rowDataset
                .select("CODE POSTAL DE LA COMMUNE DE RESIDENCE"
                        ,"NOM PRENOM OU RAISON SOCIALE"
                        ,"LIBELLE DE LA COMMUNE DE RESIDENCE","MONTANT TOTAL")
                .filter(functions.col("MONTANT TOTAL").gt(1000))
                .distinct();

    }
}
