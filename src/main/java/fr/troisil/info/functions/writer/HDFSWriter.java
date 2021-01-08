package fr.troisil.info.functions.writer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.function.Consumer;

@Slf4j
@AllArgsConstructor
public class HDFSWriter<T> implements Consumer<Dataset<T>> {

    private final String output;

    @Override
    public void accept(Dataset<T> tDataset) {

      Dataset<T> ds = tDataset.cache();

        if (output != null && !output.isEmpty())
        {
            ds.coalesce(1).write().mode(SaveMode.Overwrite).csv(output);
        }
        else
            {
            log.error("File n'existe pas ");
        }
    }
}
