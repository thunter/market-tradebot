package com.whipitupitude.tradebot;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    private DateTimeFormatter dateFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Option(names = "--kafka.properties", description = "Path to kafka.properties files", defaultValue = "kafka.properties")
    private String kafkaConfig = "kafka.properties";

    @Option(names = { "-P",
            "--position-topic" }, description = "Topic to write market positions to", defaultValue = "positions")
    private String positionTopicName;

    public Integer call() throws Exception {
        Logger logger = LoggerFactory.getLogger(TradeBot.class);

        logger.warn("Hello Consuming Students of Kafka");

        logger.trace("Creating kafka config");

        // 
        // Create properties object
        //

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

        // Create Kafka Consumer with the properties from the Properties file - note that this is typed to return TradeOpportunityAvro
        final Consumer<String, TradeOpportunityAvro> consumer = new KafkaConsumer<String, TradeOpportunityAvro>(properties);
        // Subscribe the consumer to the topic that it will consumer from
        consumer.subscribe(Arrays.asList("trades.stream.opportunities")); 
        // As this also produces back into another topic, we create a producer here as well.
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

        //
        // Main Loop
        //

        try {
            while (true) {

                // poll the topic for 1 second and return any messages received
                ConsumerRecords<String, TradeOpportunityAvro> opportunities = consumer.poll(Duration.ofMillis(1000));

                // Process each message received
                for (ConsumerRecord<String, TradeOpportunityAvro> opp : opportunities) {

                    // Get the opportunity object, which has the relevant values associated with it
                    TradeOpportunityAvro o = opp.value();


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
                                    o.getPositionQuantity() + numberToBuy, dateFmt.format(LocalDateTime.now()));
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

                        if (tradePrice < positionPrice * 0.9) { // if price has descreased 10% trigger a buy
                            /*
                             * DO SOMETHING!!! Create a new position and push it to the positions topic,
                             * possibley create a new trade
                             */
                            /* Will only descrease position by 10% */
                            int numberToSell = (int) (o.getPositionQuantity() * 0.1);
                            PositionAvro pa = new PositionAvro(o.getSymbol(), tradePrice,
                                    o.getPositionQuantity() - numberToSell, dateFmt.format(LocalDateTime.now()));
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

        producer.close();

        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new TradeBot()).execute(args);
        System.exit(exitCode);
    }

}