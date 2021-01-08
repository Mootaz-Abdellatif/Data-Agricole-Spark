package fr.troisil.info.functions.readers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DataAgricoleReaderUT {

    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;

    private static final String inputPathStr = "file://" +
            Paths.get(testConf.getString("app.data.input.path")).toAbsolutePath().toString();


    @BeforeClass
    public static void setUp(){
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }

    @Before
    public void beforeEachTest(){
        //log.info("we are about  to  run a  new test ...");
    }

    @Test
    public void testDataReader(){
        log.info("running testReader");
        log.info("Default spark fileSystem={}", sparkSession.sparkContext().hadoopConfiguration().get("fs.defaultFS"));
        log.info("sparkSession.sparkContext.hadoopConfiguration={}", sparkSession.sparkContext().hadoopConfiguration());

        Dataset<Row> ds = new DataAgricoleReader(sparkSession, inputPathStr).get();
        ds.show(10, false);
        ds.describe("MONTANT TOTAL","LIBELLE DE LA COMMUNE DE RESIDENCE").show();
        log.info("nombre total de ligne {}",ds.distinct().count());
        log.info("print schema");
        ds.printSchema();
        //log.info("count_poi={}", ds.select("Nom_du_POI").distinct().count());
        assertThat(ds.rdd().isEmpty()).isFalse();

        //log.info("running test on reader ...");
        //String testInputPath = "target/test-classes/data/inputs/data.csv";
        /*Dataset<Row> ds =
                new DataAgricoleReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        log.info("show sample with truncate=true");
        ds.show(30);
        ds.describe("MONTANT TOTAL","LIBELLE DE LA COMMUNE DE RESIDENCE").show();
        log.info("nombre total de ligne {}",ds.count());
        log.info("print schema");
        ds.printSchema();
        assertThat(ds.javaRDD().isEmpty()).isFalse();

        ds.unpersist();*/
    }


    @After
    public void afterEachTest(){
        log.info("we have just completed a test!");
    }

    @AfterClass
    public static void tearDown(){
        log.info("all tests completed!");
    }

}
