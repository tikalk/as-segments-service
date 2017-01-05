package com.tikal.fleettracker.segmentservice;

import java.util.Arrays;


import com.cyngn.kafka.consume.SimpleConsumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class SegmentMongoPersistVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SegmentMongoPersistVerticle.class);
	
	private MongoClient mongoClient;
	
	private String collectionName;	
	
	

	@Override
	public void start() {	
		collectionName = config().getJsonObject("mongoConfig").getString("segments_col_name");
		mongoClient = MongoClient.createShared(vertx, config().getJsonObject("mongoConfig"));
		if(config().getJsonObject("mongoConfig").getBoolean("recreate"))
			recreateDB();
		
		vertx.deployVerticle(SimpleConsumer.class.getName(),new DeploymentOptions().setConfig(config().getJsonObject("consumer")),this::handleKafkaDeploy);
		
		logger.info("Started listening to for Segments");
	}
	
	private void recreateDB() {
		mongoClient.runCommand("dropDatabase", new JsonObject().put("dropDatabase", 1), this::handleCommand);
		
		final JsonArray indexes = new JsonArray(Arrays.asList(
						new JsonObject().put("key", new JsonObject().put("startTime", -1)).put("name", "segments_startTime_idx"),
						new JsonObject().put("key", new JsonObject().put("vehicleId", 1).put("startTime", -1)).put("name", "segments_vehicleId_startTime_idx")
				));
		final JsonObject createIndexesCommand = new JsonObject().put("createIndexes", collectionName).put("indexes", indexes);
		mongoClient.runCommand("createIndexes", createIndexesCommand, this::handleCommand);
	}
	
	private void handleCommand(final AsyncResult<JsonObject> res) {
		if (res.succeeded())
			logger.debug(res.result().toString());
		else
			logger.error("Failed t run Command", res.cause());
	}
	
	

	
	private void persistSegmet(final Message<String> m) {
		final JsonObject segment = new JsonObject(m.body());
		logger.debug("Got segment message {}",segment);
		final Boolean isNew = (Boolean) segment.remove("isNew");
		if(isNew)
			mongoClient.insert(collectionName, segment, ar->handleSegmentAdded(isNew,segment.toString(),segment.getInteger("vehicleId"),ar));
		else
			mongoClient.save(collectionName, segment, ar->handleSegmentAdded(isNew,segment.toString(),segment.getInteger("vehicleId"),ar));
	}

	

	private void handleSegmentAdded(final Boolean isNew, final String segment, final Integer vehicleId, final AsyncResult<String> ar) {
		if (ar.succeeded()){
			if(isNew)
				logger.debug("Add a new segment in Mongo, and now will publish it. Segment is {}",segment);
			else
				logger.debug("Update existing segment in Mongo, and now will publish it. Segment is {}",segment);
			vertx.eventBus().publish("long-transits-all", segment);
			vertx.eventBus().publish("long-transits-"+vehicleId, segment);
		}
		else
			logger.error("Problem on adding Segment {}: ",segment,ar.cause());
	}

	private void handleKafkaDeploy(final AsyncResult<String> ar) {
		if (ar.succeeded()){
			logger.info("Connected to succfully to Kafka");
			vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, this::persistSegmet);
		}
		else
			logger.error("Problem connect to Kafka: ",ar.cause());
	}
	

}
