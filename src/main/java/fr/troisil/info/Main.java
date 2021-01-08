package fr.troisil.info;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.readers.DataAgricoleReader;
import fr.troisil.info.functions.stats.FilterFunction;
import fr.troisil.info.functions.writer.DataFileCsvWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
@Slf4j
public class Main
{
    public static void main( String[] args ) {

        log.info( "Starting Spark Application" );

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl).setAppName(appName)
                .setJars(new String[]{config.getString("app.data.jar.path")})
                .set("spark.executor.instances", String.valueOf(config.getInt("app.executor.nb")))
                .set("spark.executor.memory", config.getString("app.executor.memory"))
                .set("spark.executor.cores", config.getString("app.executor.cores"));

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        DataAgricoleReader reader = new DataAgricoleReader(sparkSession, config.getString("app.data.input.path"));
        FilterFunction function = new FilterFunction();
        DataFileCsvWriter<Row> writer = new DataFileCsvWriter<>(config.getString("app.data.output.path"));

        Dataset<Row> raw = reader.get();
        Dataset<Row> result = function.apply(raw);
        writer.accept(result);

        boolean enableSleep = config.getBoolean("app.data.sleep");
        log.info("isSleeping={} for 10 min", enableSleep);
        if(enableSleep){
            try {
                Thread.sleep(1000 * 60 * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("Done");

    }
}
