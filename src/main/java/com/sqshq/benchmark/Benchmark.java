package com.sqshq.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.sqshq.benchmark.config.Configuration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class Benchmark {

  public static void main(String[] args) throws IOException {

    var mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    var reader =
        Files.newBufferedReader(
            Path.of("/Users/sqshq/OSS/benchmark/src/main/java/com/sqshq/benchmark/config.yml"));

    var config = mapper.readValue(reader, Configuration.class);
    System.out.println(config);

    var pipeline =
        BsonArray.parse(
            config.Actors()
                .get(0)
                .Phases()
                .get(0)
                .Operations()
                .get(0)
                .OperationCommand()
                .pipeline()
                .toString());

    var collectionName =
        config.Actors().get(0).Phases().get(0).Operations().get(0).OperationCommand().aggregate();

    ConnectionString connectionString = new ConnectionString("...");
    var settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
    var mongoClient = MongoClients.create(settings);
    MongoDatabase database = mongoClient.getDatabase("test");

    var registry = new MetricRegistry();

    var timer = registry.timer("query7");
    var context = timer.time();
    var command =
        new BsonDocument()
            .append("aggregate", new BsonString(collectionName))
            .append("pipeline", pipeline)
            .append("cursor", new BsonDocument());

    var result = database.runCommand(command);
    context.stop();

    ConsoleReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build()
        .report();

    System.out.println(result);
  }
}
