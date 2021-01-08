package fr.troisil.info.functions.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
@Slf4j
@RequiredArgsConstructor
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogName;
    @Override
    public void accept(Dataset<Row> rowDataset) {
        try {
            String catalogPathSTr = getClass().getClassLoader().getResource(catalogName).getPath();
            String catalog = String.join("\n", Files.readAllLines(Paths.get(catalogPathSTr), Charset.defaultCharset()));
            log.info("catalog=\n{}",catalog);
            log.info("writing data into hbase");
            rowDataset
                .write()
                .option(HBaseTableCatalog.tableCatalog(),catalog).option(HBaseTableCatalog.newTable(),"5")
                .mode("overwrite")
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
            log.info("done");
        }   catch (IOException ioException){
            ioException.printStackTrace();
        }

    }
}
