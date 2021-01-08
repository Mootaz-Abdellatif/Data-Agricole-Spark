package fr.troisil.info.functions.readers;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class HiveReader implements Supplier<Dataset<Row>> {
    private final String dbName;
    private final String tableName;
    @NonNull
    private final SparkSession sparkSession;

    @Override
    public Dataset<Row> get() {

        String fullTableName = String.format("%s.%s", dbName, tableName);
        //String fullDbDataPathStr = String.format("%s/%s.db", location, dbName);

        String createDbQuery = String.format("SELECT * FROM %s", fullTableName);
        log.info("select datab using createDbQuery={}...", createDbQuery);
        sparkSession.sql(createDbQuery);
        log.info("reading data into hive table = {}...", fullTableName);
        log.info("data ={}",sparkSession.sql(createDbQuery));

        log.info("done!!");

        return sparkSession.sql(String.format("SELECT * from %s.%s",dbName,tableName));
    }
}
