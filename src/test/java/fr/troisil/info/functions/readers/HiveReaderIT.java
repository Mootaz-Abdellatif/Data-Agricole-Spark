package fr.troisil.info.functions.readers;

import fr.troisil.info.functions.writer.HiveWriter;
import fr.troisil.info.functions.writer.HiveWriterIT;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
public class HiveReaderIT {
    private final String dbName = "dataAgricole";

    private final String tableName = "data";

    //private final String location = "/tmp/hive/datawarehouse";


    @Test
    public void testReader(){
        log.info("running hiveReader test");
        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("test-writer")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> actualData = new HiveReader(dbName, tableName,sparkSession).get();
        log.info("data: {}",actualData.collectAsList());
        actualData.show();

        assertThat(actualData.rdd().isEmpty()).isFalse();
    }
}
