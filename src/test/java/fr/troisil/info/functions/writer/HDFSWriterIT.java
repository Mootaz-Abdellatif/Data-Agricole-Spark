package fr.troisil.info.functions.writer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.readers.DataAgricoleReader;
import fr.troisil.info.functions.stats.FilterFunction;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@AllArgsConstructor
public class HDFSWriterIT {

    private static SparkSession sparkSession;
    private static final Configuration hadoopConf = new Configuration();

    private static final Config testConf = ConfigFactory.load("application.conf");
    private static final String inputPathStr = testConf.getString("app.data.input.path");


    private static final Path inputPath = new Path(inputPathStr);

    private static FileSystem hdfs;

    private static void clean() throws IOException {
        if(hdfs != null){
            hdfs.delete(inputPath, true);
        }
    }
    @BeforeClass
    public static void setUp() throws IOException{
        log.info("init hdfs");
        hdfs = FileSystem.get(hadoopConf);
        clean();
        hdfs.mkdirs(inputPath.getParent());
        hdfs.copyFromLocalFile(inputPath, inputPath);
        assertThat(hdfs.exists(inputPath)).isTrue();
        assertThat(hdfs.listFiles(inputPath, true).hasNext()).isTrue();
        sparkSession = SparkSession.builder().master("local[2]").appName("test-reader").getOrCreate();
    }

    @Before
    public void beforeEachTest(){
        log.info("we are about  to  run a  new test ...");
    }

    @Test
    public void testWriter() throws IOException {
        log.info("running DataAgricoleWriterWithHadoopUnitUT.testWriter");
        log.info("Default hdfs fileSystem={}", hdfs.getConf().get("fs.defaultFS"));
        log.info("Default spark fileSystem={}", sparkSession.sparkContext().hadoopConfiguration().get("fs.defaultFS"));
        log.info("sparkSession.sparkContext.hadoopConfiguration={}", sparkSession.sparkContext().hadoopConfiguration());

        HDFSWriter<Row> writer = new HDFSWriter<>(testConf
                .getString("app.data.output.path"));
        Dataset<Row> ds = new DataAgricoleReader(sparkSession, inputPathStr).get();
        writer.accept(ds);
        log.info("writing into hdfs");
        //assertThat(Files.exists(Paths.get(ConfigFactory.load("application.conf").getString("app.data.output.path")))).isFalse();
        assertThat(ds.rdd().isEmpty()).isFalse();
        assertThat(hdfs.exists(inputPath)).isTrue();

        log.info("success test writer !!!");
    }


    @After
    public void afterEachTest(){
        log.info("we have just completed a test!");
    }

    @AfterClass
    public static void tearDown() throws IOException{
        log.info("all tests completed!");
        clean();
    }
}
