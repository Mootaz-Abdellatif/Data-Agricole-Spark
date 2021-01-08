package fr.troisil.info.functions.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class CustomFlatMapFunction implements Function<String, Iterator<String>>, Serializable {

    @Override
    public Iterator<String> apply(String str) {
        log.info("applying my custom flatmap java function");

        Iterator<String> it = Arrays.stream(str.split("\\W"))
                .collect(Collectors.toList())
                .iterator();

        return it;
    }
}
