package fr.troisil.info.functions.writer;

import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.Test;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
public class HBaseWriterIT {
    private final String catalogName = "data-catalog.json";
    private final String catalog = "{\n" +
            "  \"table\": {\n" +
            "    \"namespace\": \"dataAgricole\",\n" +
            "    \"name\": \"data\"\n" +
            "  },\n" +
            "  \"rowkey\": \"key\",\n" +
            "  \"columns\": {\n" +
            "    \"key\": {\n" +
            "      \"cf\": \"rowkey\",\n" +
            "      \"col\": \"key\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE\": {\n" +
            "      \"cf\": \"commune\",\n" +
            "      \"col\": \"LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"MONTANT_TOTAL\": {\n" +
            "      \"cf\": \"commune\",\n" +
            "      \"col\": \"MONTANT_TOTAL\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"NOM_PRENOM_OU_RAISON_SOCIALE\": {\n" +
            "      \"cf\": \"commune\",\n" +
            "      \"col\": \"NOM_PRENOM_OU_RAISON_SOCIALE\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
    @Test
    public void testWriter(){
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("test-reader")
                .getOrCreate();
        List<HBaseRow> expected = Arrays.asList(
                HBaseRow.builder().key("k1").MONTANT_TOTAL("56").LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("PREMONT").NOM_PRENOM_OU_RAISON_SOCIALE("GAEC PARTIEL DE LA MALADRERIE").build(),
                HBaseRow.builder().key("k2").MONTANT_TOTAL("26").LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("CHAPELLE MONTHODON").NOM_PRENOM_OU_RAISON_SOCIALE("SCL ROULOT VEROT").build()
                );
        Dataset<Row> expectedData = sparkSession.createDataset(expected, Encoders.bean(HBaseRow.class)).toDF();
        expectedData.printSchema();
        expectedData.show();
        new HBaseWriter(catalogName).accept(expectedData);
        log.info("checking insertion !!!");
        Dataset<Row> rowd = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load();
        rowd.show();
        log.info("creating log info object !!!");
        Dataset<HBaseRow> actualData = rowd.as(Encoders.bean(HBaseRow.class));
        assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
    }
    @Data @Builder @AllArgsConstructor @NoArgsConstructor
    public static class HBaseRow implements Serializable {
        private String key;
        private String MONTANT_TOTAL;
        private String LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE;
        private String NOM_PRENOM_OU_RAISON_SOCIALE;
    }

}
