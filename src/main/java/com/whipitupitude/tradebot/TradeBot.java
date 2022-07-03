package com.whipitupitude.tradebot;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whipitupitude.market.PositionAvro;
import com.whipitupitude.market.TradeOpportunityAvro;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class TradeBot implements Callable<Integer> {

    @Option(names = "--kafka.properties", description = "Path to kafka.properties files", defaultValue = "kafka.properties")
    private String kafkaConfig = "kafka.properties";

    @Option(names = { "-P",
            "--position-topic" }, description = "Topic to write market positions to", defaultValue = "positions")
    private String positionTopicName;

    public Integer call() throws Exception {
        Logger logger = LoggerFactory.getLogger(TradeBot.class);

        logger.warn("Hello Consuming Students of Kafka");

        logger.trace("Creating kafka config");
        Properties properties = new Properties();
        try {
            if (!Files.exists(Paths.get(kafkaConfig))) {
                throw new IOException(kafkaConfig + " not found");
            } else {
                try (InputStream inputStream = new FileInputStream(kafkaConfig)) {
                    properties.load(inputStream);
                }
            }

        } catch (Exception e) {
            logger.error("Cannot configure Kafka " + kafkaConfig);
            throw new RuntimeException(e);
        }

        final Consumer<String, TradeOpportunityAvro> consumer = new KafkaConsumer<String, TradeOpportunityAvro>(
                properties);
        consumer.subscribe(Arrays.asList("trades.stream.opportunities"));
        final Producer<String, Object> producer = new KafkaProducer<>(properties);

        // Registering a shutdown hook so we can exit cleanly
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can
                // safely do to a consumer is wake it up
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, TradeOpportunityAvro> opportunities = consumer.poll(Duration.ofMillis(1000));
                logger.info("!!!!!! STARTING WHILE LOOP");
                for (ConsumerRecord<String, TradeOpportunityAvro> opp : opportunities) {
                    TradeOpportunityAvro o = opp.value();
                    logger.info("!!!!! - got opp " + opp);
                    if (o.getBuySell().toString().equals("B")) {
                        Double positionPrice = o.getPositionPrice();
                        Double tradePrice = o.getTradePrice();

                        logger.debug("!!!!!!!!! - found trade " + o.getSymbol().toString() + " positionPrice:"
                                + positionPrice + " tradePrice:" + tradePrice);

                        if (tradePrice > positionPrice * 1.1) { // if price has increased 10% trigger a buy
                            /*
                             * DO SOMETHING!!! Create a new position and push it to the positions topic,
                             * possibley create a new trade
                             */
                            /* Will only increase position by 10% */
                            int numberToBuy = (int) (o.getPositionQuantity() * 0.1);
                            PositionAvro pa = new PositionAvro(o.getSymbol(), tradePrice,
                                    o.getPositionQuantity() + numberToBuy, System.currentTimeMillis());
                            ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(
                                    positionTopicName, opp.key(), pa);
                            logger.debug("!!!!!!!! - sending new position record!!!! " + record);
                            producer.send(record);
                        }

                    } else { // sell side !!!
                        Double positionPrice = o.getPositionPrice();
                        Double tradePrice = o.getTradePrice();

                        logger.debug("!!!!!!!!! - found sell trade " + o.getSymbol().toString() + " positionPrice:"
                                + positionPrice + " tradePrice:" + tradePrice);

                        if (tradePrice > positionPrice * 1.1) { // if price has increased 10% trigger a buy
                            /*
                             * DO SOMETHING!!! Create a new position and push it to the positions topic,
                             * possibley create a new trade
                             */
                            /* Will only increase position by 10% */
                            int numberToBuy = (int) (o.getPositionQuantity() * 0.1);
                            PositionAvro pa = new PositionAvro(o.getSymbol(), tradePrice,
                                    o.getPositionQuantity() + numberToBuy, System.currentTimeMillis());
                            ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(
                                    positionTopicName, opp.key(), pa);
                            logger.debug("!!!!!!!! - sending new position record!!!! " + record);
                            producer.send(record);
                        }
                    }

                }
            }
        } catch (

        WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }

        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new TradeBot()).execute(args);
        System.exit(exitCode);
    }

}