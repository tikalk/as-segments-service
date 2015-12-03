package com.tikal.angelsense.segmentservice;

import com.cyngn.kafka.MessageConsumer;

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
		final JsonObject newSegment = new JsonObject(m.body());
		logger.debug("Got segment message {}",newSegment);
		if(redis==null)
			redis = RedisClient.create(vertx, config);
		redis.getset("segment."+newSegment.getString("id"), newSegment.toString(), ar->handleFetchedCurrentSegment(newSegment,ar));
	}

	
	
	private void handleFetchedCurrentSegment(final JsonObject newSegment, final AsyncResult<String> currentSegmentAr) {
		if (currentSegmentAr.succeeded()){
			redis.zadd("segments.ids.angel."+(newSegment.getInteger("angelId")), newSegment.getLong("startTime"), newSegment.getString("id"), zaAr->handleSegmentAdded(newSegment.toString(),zaAr,currentSegmentAr.result()));
		}else
			logger.error("Problem On ZRange for {}: ",newSegment,currentSegmentAr.cause());
	}


	private void handleSegmentAdded(final String segment, final AsyncResult<Long> ar, final String previousSegment) {
		if (ar.succeeded()){
			if(previousSegment==null)
				logger.debug("Add a new segment in Redis, and now will publish it. Segment is {}",segment);
			else
				logger.debug("Update existing segment in Redis, and now will publish it. Segment is {}. previous segment is ",segment);
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
