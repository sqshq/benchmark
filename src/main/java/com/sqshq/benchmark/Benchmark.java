package com.sqshq.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.sqshq.benchmark.config.Configuration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class Benchmark {

  // arg0 - full path to yml config
  // arg1 - mongodb connection string
  public static void main(String[] args) throws IOException, InterruptedException {

    var mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    var reader = Files.newBufferedReader(Path.of(args[0]));

    var mongoClient =
        MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(args[1]))
                .build());
    var config = mapper.readValue(reader, Configuration.class);
    var metrics = new MetricRegistry();

    var actor = config.Actors().get(0); // support only single actor for now

    var pool = Executors.newFixedThreadPool(actor.Threads());

    for (var phase : actor.Phases()) {

      var latch = new CountDownLatch(actor.Threads());

      for (int i = 0; i < actor.Threads(); i++) {

        var operation = phase.Operations().get(0); // support only single operation for now
        var database = mongoClient.getDatabase(phase.Database());
        var collection = operation.OperationCommand().aggregate();

        System.out.printf("Starting thread %s for %s %n", i, operation.OperationMetricsName());

        pool.submit(
            () -> {
              int minutes = new Scanner(phase.Duration()).useDelimiter("\\D+").nextInt();

              if (!phase.Duration().contains("minute")) {
                throw new IllegalArgumentException("Duration is expected to be only in minutes");
              }

              var endTime = Instant.now().plus(Duration.ofMinutes(minutes));

              while (Instant.now().isBefore(endTime)) {

                var pipeline = BsonArray.parse(operation.OperationCommand().pipeline().toString());

                var timer = metrics.timer(operation.OperationMetricsName());
                var context = timer.time();
                var command =
                    new BsonDocument()
                        .append("aggregate", new BsonString(collection))
                        .append("pipeline", pipeline)
                        .append("cursor", new BsonDocument());
                var result = database.runCommand(command, BsonDocument.class);
                if (ThreadLocalRandom.current().nextInt(0, 25) == 10) {
                  System.out.printf(
                      "One of the queries result size was %s %n",
                      result.getDocument("cursor").getArray("firstBatch").size());
                }
                context.stop();
              }

              latch.countDown();
            });
      }

      latch.await();
    }

    ConsoleReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build()
        .report();

    pool.shutdown();
  }
}
