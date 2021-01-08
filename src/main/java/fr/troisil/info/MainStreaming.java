package fr.troisil.info;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.KafkaReceiver;
import fr.troisil.info.functions.writer.HBaseWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Hello world!
 *
 */
@Slf4j
public class MainStreaming
{
    public static void main( String[] args ) throws InterruptedException {

        log.info( "Starting Spark Application" );

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl).setAppName(appName)
                //.setJars(new String[]{config.getString("app.data.jar.path")})
                .set("spark.executor.instances", String.valueOf(config.getInt("app.executor.nb")))
                .set("spark.executor.memory", config.getString("app.executor.memory"))
                .set("spark.executor.cores", config.getString("app.executor.cores"));

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);

        final JavaStreamingContext jsc = new JavaStreamingContext(javaSparkContext, new Duration(1000*10));
        KafkaReceiver kafkaReceiver = new KafkaReceiver(jsc);

        JavaDStream<String> stringJavaDStream = kafkaReceiver.get();
        stringJavaDStream.foreachRDD(
                stringJavaRDD -> {
                    if(stringJavaRDD.isEmpty()){
                        log.info("no data received!");
                    } else {
                        Metadata md = new MetadataBuilder().build();
                        StructType schema = new StructType(new StructField[]{
                                new StructField("key", DataTypes.StringType, true, md),
                                new StructField("LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE", DataTypes.StringType, true, md),
                                new StructField("MONTANT_TOTAL", DataTypes.StringType, true, md),
                                new StructField("NOM_PRENOM_OU_RAISON_SOCIALE", DataTypes.StringType, true, md)
                        }
                        );
                        JavaRDD<Row> rowRDD = stringJavaRDD.map(str -> {
                            String[] fields = str.split(":", 4);
                            return RowFactory.create(fields[0], fields[1], fields[2], fields[3]);
                        });
                        Dataset<Row> raw2 = sparkSession.createDataFrame(rowRDD, schema);

                        Dataset<Row> clean = raw2.filter(raw2.col("key").isNotNull());

                        clean.foreach(
                                new ForeachFunction<Row>() {
                                    @Override
                                    public void call(Row s) throws Exception {
                                        log.info("found = {}", s);
                                    }
                                }
                        );
                        new HBaseWriter("data-catalog.json").accept(clean);
                    }
                }
        );


        log.info("Done");

        jsc.start();
        jsc.awaitTermination();


    }
}
