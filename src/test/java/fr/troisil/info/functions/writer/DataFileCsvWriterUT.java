package fr.troisil.info.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.readers.DataAgricoleReader;
import fr.troisil.info.functions.stats.FilterFunction;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
@AllArgsConstructor
public class DataFileCsvWriterUT {


    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;
    private static Dataset<Row> ds;
    @BeforeClass
    public static void setUp(){
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
        String testInputPath = "target/test-classes/data/inputs/data.csv";
        ds =
                new DataAgricoleReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
    }

    @Before
    public void beforeEachTest(){
        log.info("we are about  to  run a  new test ...");
    }



    @Test
    public void testWriter(){
        log.info("running test on writer Csv ...");

        DataFileCsvWriter<Row> writer = new DataFileCsvWriter<>(testConf
                .getString("app.data.output.path"));

        Dataset<Row> ds1 = new FilterFunction().apply(ds);
        writer.accept(ds1);

        assertThat(Files.exists(Paths.get(ConfigFactory.load("application.conf").getString("app.data.output.path")))).isTrue();


        log.info("success test writer !!!");
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
