package com.example.bigdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StockDataProcessing {

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-data-etl");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        int ANOMALY_DAYS = Integer.parseInt(args[1]);
        float ANOMALY_DIFF = Integer.parseInt(args[2])/100.0f;

        final Serde<String> stringSerde = Serdes.String();
        final Serde<StockResultRecord> stockResultSerde = Serdes
                .serdeFrom(new StockResultRecordSerializer(),
                        new StockResultRecordDeserializer());
        final Serde<StockAggregated> stockAggSerde = Serdes
                .serdeFrom(new StockAggregatedSerializer(),
                        new StockAggregatedDeserializer());
        final StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> symbolsNames = builder
                .stream("symbols", Consumed.with(stringSerde, stringSerde))
                .map((key, line) -> {
                    String[] splittedLine = line.split(",");
                    return KeyValue.pair(splittedLine[1], splittedLine[2]);
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue,
                        Materialized.as("symbolsNames"));

        KStream<String, StockResultRecord> stockResultsStream = builder
                .stream("stocks-results", Consumed.with(stringSerde, stringSerde))
                .filter((key, line) -> StockResultRecord.lineIsCorrect(line))
                .map((key, line) ->  {
                    StockResultRecord record = StockResultRecord.parseFromLine(line);
                    return KeyValue.pair(record.getStock(), record);
                })
                .join(symbolsNames, (stockRecord, name) -> {
                    stockRecord.setStockName(name);
                    return stockRecord;
                }, Joined.with(stringSerde, stockResultSerde, stringSerde));

        KTable<String, StockAggregated> stockAggregates = stockResultsStream
                .map((key, value) -> KeyValue.pair(
                        value.getMonth() + ";" + value.getStock() + ";" + value.getStockName(), value))
                .groupByKey(Grouped.with(stringSerde, stockResultSerde))
                .aggregate(() -> new StockAggregated(0, 0, -1, 0, 0),
                        (key, value, aggregated) -> {
                            aggregated.update(value);
                            return aggregated;
                        },
                        Materialized.with(stringSerde, stockAggSerde));

        stockAggregates
                .toStream()
                .map((key, agg) -> KeyValue.pair(key, stockAggToJson(key, agg)))
                .to("aggregated");

        KTable<Windowed<String>, StockAggregated> stockMinMax = stockResultsStream
                .map((key, value) -> KeyValue.pair(value.getStock() + ";" + value.getStockName(), value))
                .groupByKey(Grouped.with(stringSerde, stockResultSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(ANOMALY_DAYS)).advanceBy(Duration.ofDays(1)))
                .aggregate(() -> new StockAggregated(-1, 0),
                        (key, value, aggregated) -> {
                            aggregated.updateMinMax(value);
                            return aggregated;
                        },
                        Materialized.with(stringSerde, stockAggSerde));

        KStream<String, String> anomalyStocks = stockMinMax
                .toStream()
                .filter((key, value) -> value.getMinMaxDiff() > ANOMALY_DIFF)
                .map((key, value) -> {
                    String startDate = timestampToDate(key.window().start());
                    String endDate = timestampToDate(key.window().end());
                    return KeyValue.pair(key.key() + ";" + startDate + ";" + endDate,
                            stockAnomalyToJson(key.key(), value, startDate, endDate));
                });

        anomalyStocks
                .to("anomaly-stocks");

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static String timestampToDate(long timestamp) {
        LocalDate date = LocalDate.ofEpochDay((timestamp/86400000) - 3653);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return date.format(formatter);
    }

    private static String stockAggToJson(String key, StockAggregated agg) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        node.put("close_avg", agg.getCloseSum()/agg.getCloseCount());
        node.put("low", agg.getLowMin());
        node.put("high", agg.getHighMax());
        node.put("volume_sum", agg.getVolumeSum());

        String[] splittedKey = key.split(";");
        String[] splittedMonth = splittedKey[0].split("-");
        node.put("year", splittedMonth[0]);
        node.put("month", splittedMonth[1]);
        node.put("stock", splittedKey[1]);
        node.put("stock_name", splittedKey[2]);
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    private static String stockAnomalyToJson(String key, StockAggregated agg, String startDate, String endDate) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        node.put("start_date", startDate);
        node.put("end_date", endDate);
        node.put("low", agg.getLowMin());
        node.put("high", agg.getHighMax());
        node.put("high_low_diff", agg.getMinMaxDiff());

        String[] splittedKey = key.split(";");
        node.put("stock_name", splittedKey[1]);
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    static class StockResultRecordSerializer implements Serializer<StockResultRecord> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, StockResultRecord stockResultRecord) {
            StringSerializer stringSerializer = new StringSerializer();
            return stringSerializer.serialize(topic, stockResultRecord.toString());
        }

        @Override
        public void close() {

        }
    }

    static class StockResultRecordDeserializer implements Deserializer<StockResultRecord> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public StockResultRecord deserialize(String topic, byte[] bytes) {
            StringDeserializer stringDeserializer = new StringDeserializer();
            String stockResultString = stringDeserializer.deserialize(topic, bytes);
            return StockResultRecord.fromString(stockResultString);
        }

        @Override
        public void close() {

        }
    }



    static class StockAggregatedSerializer implements Serializer<StockAggregated> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, StockAggregated stockAggregated) {
            StringSerializer stringSerializer = new StringSerializer();
            return stringSerializer.serialize(topic, stockAggregated.toString());
        }

        @Override
        public void close() {

        }
    }

    static class StockAggregatedDeserializer implements Deserializer<StockAggregated> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public StockAggregated deserialize(String topic, byte[] bytes) {
            StringDeserializer stringDeserializer = new StringDeserializer();
            String stockAggString = stringDeserializer.deserialize(topic, bytes);
            return StockAggregated.fromString(stockAggString);
        }

        @Override
        public void close() {

        }
    }
}
