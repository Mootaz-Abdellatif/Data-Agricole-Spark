package fr.troisil.info.functions.stats;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.readers.DataAgricoleReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class MaxMontantFunctionUT {

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
    public void testFilter(){


        log.info("Commune de Residence avec le Montant maximal =  {} ," +
                " Nombre totals des personnes = {} ",
                new MaxMontantFunction().apply(ds),ds.count());


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
