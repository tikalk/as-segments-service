package com.tikal.angelsense.segmentservice;

import com.cyngn.kafka.MessageConsumer;
import com.cyngn.kafka.MessageProducer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class SegmentPersistVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SegmentPersistVerticle.class);
	private RedisClient redis;
	private RedisOptions config;

	@Override
	public void start() {	
		vertx.deployVerticle(MessageConsumer.class.getName(),new DeploymentOptions().setConfig(config()),this::handleKafkaDeploy);
		config = new RedisOptions().setHost(config().getString("redis-host"));
		logger.info("Started listening to for Segments");
	}
	
	
	
	private void persistSegmet(final Message<String> m) {
		final JsonObject segment = new JsonObject(m.body());
		logger.debug("Got segment message {}",segment);
		if(redis==null)
			redis = RedisClient.create(vertx, config);
		redis.zadd("segment.angel."+segment.getInteger("angelId"), segment.getLong("startTime").doubleValue(), segment.toString(), ar->handleAddSegment(segment.toString(),ar));
	}

	private void handleAddSegment(final String segment, final AsyncResult<Long> ar) {
		if (ar.succeeded()){
			logger.debug("Added Segment to Redis. Segment is {}",segment);
			vertx.eventBus().publish("segments-feed", segment);
		}
		else
			logger.error("Problem on adding Segment {}: ",segment,ar.cause());
	}

	private void handleKafkaDeploy(final AsyncResult<String> ar) {
		if (ar.succeeded()){
			logger.info("Connected to succfully to Kafka");
			vertx.eventBus().consumer(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS, this::persistSegmet);
		}
		else
			logger.error("Problem connect to Kafka: ",ar.cause());
	}
	

}
