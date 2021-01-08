package fr.troisil.info.functions.stats;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import static org.assertj.core.api.Assertions.assertThat;


import java.util.Arrays;

@Slf4j
public class DataFilterPostalUT {

    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;
    @BeforeClass
    public static void setUp(){
        //log.info("initializing sparkSession ...");
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
    public void testFilter(){

        String testInputPath = "target/test-classes/data/inputs/data.csv";
        /*Dataset<Row> ds =
                new DataAgricoleReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        */
        log.info("show dataset before filter");
        Dataset<Record> dataset = sparkSession
                .createDataset(
                        Arrays.asList(new Record("comm", "11"),
                                new Record("comm2", "88")),
                        Encoders.bean(Record.class)
                );
        Dataset<Row> ds = dataset.toDF();
        ds.printSchema();
        Dataset<Row> ds1 = ds.withColumnRenamed("LibelleCommune",
                "LIBELLE DE LA COMMUNE DE LA RESIDENCE")
                .withColumnRenamed("montant",
                "MONTANT TOTAL");
        ds1.printSchema();
        ds.show();
        Dataset<Row> dsfilter =
                new FilterFunction().apply(ds1);
        log.info("show sample with truncate=true");
        dsfilter.show();
        dsfilter.printSchema();
        ds.count();
        Long mtaides = dsfilter.head().getAs("MONTANT TOTAL DES AIDES");
        assertThat(mtaides).isEqualTo(10);
    }
    @Test
    public void testFilter2(){

        String testInputPath = "target/test-classes/data/inputs/data.csv";
        /*Dataset<Row> ds =
                new DataAgricoleReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        */
        log.info("show dataset before filter");
        Dataset<Record> dataset = sparkSession
                .createDataset(
                        Arrays.asList(new Record("comm", "1"),
                                new Record("comm2", "1")),
                        Encoders.bean(Record.class)
                );
        Dataset<Row> ds = dataset.toDF();
        ds.printSchema();
        Dataset<Row> ds1 = ds.withColumnRenamed("LibelleCommune",
                "LIBELLE DE LA COMMUNE DE LA RESIDENCE").withColumnRenamed("montant",
                "MONTANT TOTAL");
        ds1.printSchema();
        ds.show();
        Dataset<Row> dsfilter =
                new FilterFunction().apply(ds1);
        log.info("show sample with truncate=true");
        dsfilter.show();
        dsfilter.printSchema();
        assertThat(ds.count()).isEqualTo(1);

    }

    @After
    public void afterEachTest(){
        log.info("we have just completed a test!");
    }

    @AfterClass
    public static void tearDown() {
        log.info("all tests completed!");
    }
}
