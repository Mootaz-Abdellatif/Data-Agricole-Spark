package fr.troisil.info;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.troisil.info.functions.writer.HiveWriterIT;
import fr.troisil.info.utils.dataSample;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
@Slf4j
public class MainStreamingIT
{

        private final Config config = ConfigFactory.load("application.conf");

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
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"NOM_PRENOM_OU_RAISON_SOCIALE\": {\n" +
                "      \"cf\": \"commune\",\n" +
                "      \"col\": \"NOM_PRENOM_OU_RAISON_SOCIALE\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        @Test
        public void runAppMain() throws InterruptedException {

            Properties props = new Properties();
            props.put("bootstrap.servers", config.getString("app.kafka.brokers"));
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            List<String> topics = config.getStringList("app.kafka.topics");

            // Expected
            List<dataSample> expected = Arrays.asList(
                    dataSample.builder().key("k1").MONTANT_TOTAL(56.0).LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("PREMONT").NOM_PRENOM_OU_RAISON_SOCIALE("GAEC PARTIEL DE LA MALADRERIE").build(),
                    dataSample.builder().key("k2").MONTANT_TOTAL(26.0).LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE("CHAPELLE MONTHODON").NOM_PRENOM_OU_RAISON_SOCIALE("SCL ROULOT VEROT").build()
            );

            // Producer
            Producer<String, String> producer = new KafkaProducer<>(props);
            expected.forEach(dataSample -> {
                log.info("sending hBaseRow={}", dataSample);
                producer.send(new ProducerRecord<String, String>(topics.get(0), dataSample.toKafkaMessage()));
            });
            producer.close();

            // Consumer
            MainStreaming.main(new String[]{});

            // Assert
            Dataset<dataSample> actualData = SparkSession.builder().master("local[2]").appName("test-app-streaming").getOrCreate()
                    .read()
                    .option(HBaseTableCatalog.tableCatalog(), catalog)
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .load().as(Encoders.bean(dataSample.class));
            actualData.printSchema();
            actualData.show();
            assertThat(actualData.collectAsList()).containsExactlyElementsOf(expected);
        }
        // Thread
        //MainStreaming.main(new String[]{});
        //assertTrue( true );

}
