package fr.troisil.info.functions.readers;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;
@Slf4j
@AllArgsConstructor
public class DataAgricoleReader implements Supplier<Dataset<Row>>{
    private final SparkSession sparkSession;
    private final String inputPathStr;

    @Override
    public Dataset<Row> get() {
        log.info("reading data from inputPathStr = {}", inputPathStr);

        return sparkSession.read()
                .option("delimiter",";")
                .option("header",true)
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .load(inputPathStr);
    }
}
