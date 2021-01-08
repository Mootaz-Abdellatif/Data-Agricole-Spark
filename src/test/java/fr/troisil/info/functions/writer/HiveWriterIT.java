package fr.troisil.info.functions.writer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HiveWriterIT {

    private final String dbName = "dataAgricole";

    private final String tableName = "data";

    private final String location = "/tmp/hive/datawarehouse";


    @Test
    public void testWriter(){
        log.info("running hiveWriter test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer")
                .enableHiveSupport()
                .getOrCreate();

        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("k1").MONTANT_TOTAL(56.0).LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("PREMONT").NOM_PRENOM_OU_RAISON_SOCIALE("GAEC PARTIEL DE LA MALADRERIE").build(),
                HBaseRow.builder().key("k2").MONTANT_TOTAL(26.0).LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("CHAPELLE MONTHODON").NOM_PRENOM_OU_RAISON_SOCIALE("SCL ROULOT VEROT").build()
        );

        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();

        log.info("showing databases before...");
        sparkSession.sql("show databases").show();

        new HiveWriter(dbName, tableName, location).accept(expectedData);

        log.info("showing databases after...");
        sparkSession.sql("show databases").show();

        Dataset<HBaseRow> actualData = sparkSession.sql(String.format("SELECT * from %s.%s", dbName, tableName))
                .as(Encoders.bean(HBaseRow.class));

        assertThat(actualData.collectAsList()).containsExactlyInAnyOrder(expected.toArray(new HBaseRow[0]));
    }

    @Data @Builder @AllArgsConstructor @NoArgsConstructor
    public static class HBaseRow implements Serializable {
        private String key;
        private Double MONTANT_TOTAL;
        private String LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE;
        private String NOM_PRENOM_OU_RAISON_SOCIALE;
    }
}
