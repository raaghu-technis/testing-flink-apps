package com.technis.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DataStreamJobTest {

    private static DataStream<Long> buildPipeline(StreamExecutionEnvironment env) {
        return env.fromSequence(1L, 10L)
                .filter(v -> v % 2 == 0);
    }

    @Test
    void collectsOnlyEvenNumbers() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        DataStream<Long> evens = buildPipeline(env);

        List<Long> result = new ArrayList<>();
        try (CloseableIterator<Long> it = evens.executeAndCollect()) {
            it.forEachRemaining(result::add);
        }

        Assertions.assertEquals(5, result.size(), "Should emit five even numbers");
        Assertions.assertIterableEquals(List.of(2L, 4L, 6L, 8L, 10L), result);
    }
}