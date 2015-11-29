package com.tikal.angelsense.segmentservice;

import com.cyngn.kafka.MessageConsumer;
import com.cyngn.kafka.MessageProducer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
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
	
	private String getKey(final int angelId){
		return "segment.angel."+angelId;
	}
	
	private long getReadingTime(final JsonObject seg){
		return seg.getJsonObject("lastGPSInInterval").getLong("readingTime");
	}
	
	private void persistSegmet(final Message<String> m) {
		final JsonObject segmentInterval = new JsonObject(m.body());
		logger.debug("Got segment message {}",segmentInterval);
		if(redis==null)
			redis = RedisClient.create(vertx, config);
		final int angelId = segmentInterval.getInteger("angelId");
//		redis.zcount(getKey(angelId), 0, -1, ar->handleCountedSegments(angelId,segmentInterval,ar));
		redis.zrevrange(getKey(angelId), 0, -1,null, ar->handleFetchedCurrentSegment(angelId,segmentInterval,ar));
	}

//	private void handleCountedSegments(final int angelId,final JsonObject segmentInterval, final AsyncResult<Long> ar) {
//		if (ar.succeeded()){
//			final Long zcount = ar.result();
//			if(zcount==0){
//				logger.debug("As there is no members in for angelId {}, we will just add the interval as a new segment");
//				redis.zadd(getKey(angelId), getReadingTime(segmentInterval), segmentInterval.toString(), zaAr->handleSegmentAdded(segmentInterval.toString(),zaAr));
//			}
//			else{
//				logger.debug("As there are members in for angelId {}, we check if we need to merge the new interval or add it as a new one ");
//				redis.zrange(getKey(angelId), zcount-1, zcount, zrangeAr->handleFetchedCurrentSegment(angelId,segmentInterval,zrangeAr));
//			}
//		}
//		else
//			logger.error("Problem On Zcount for {}: ",segmentInterval,ar.cause());
//	}



	private void handleFetchedCurrentSegment(final int angelId, final JsonObject segmentInterval, final AsyncResult<JsonArray> zrangeAr) {
		if (zrangeAr.succeeded()){
			final JsonArray resArray = zrangeAr.result();
			if(resArray.size()==0){
				logger.debug("As there is no members in for angelId {}, we will just add the interval as a new segment");
				redis.zadd(getKey(angelId), getReadingTime(segmentInterval), segmentInterval.toString(), zaAr->handleSegmentAdded(segmentInterval.toString(),zaAr));
			}else{//There are members for this angel
				final JsonObject currentSegment = resArray.getJsonObject(0);
				final String currentSegmentType = currentSegment.getString("segmentType");
				if(!currentSegmentType.equals(segmentInterval.getString("segmentType"))){
					logger.debug("As the cuurent segment and new interval have diifferent types, we will create the new interval as a new segment");
					redis.zadd(getKey(angelId), getReadingTime(segmentInterval), segmentInterval.toString(), ar->handleSegmentAdded(segmentInterval.toString(),ar));
				}
				else{
					//They are the same type
					if(currentSegmentType.equals("transit")){//We need to update the existing transit
						logger.debug("Both current segment and interval have transit type. We need to merge them by putting the lastGpsInterval in current segment");
						redis.zrem(getKey(angelId), currentSegment.toString(), remAr->handleSegmentRemoved(angelId, segmentInterval,currentSegment,remAr));
					}else{//They are both have 'place'
						logger.debug("Both current segment and interval have place type. We will check the id of the last's segment gps and first interval gps");
						if(currentSegment.getJsonObject("lastGPSInInterval").getString("id").equals(segmentInterval.getJsonObject("firstGPSInInterval").getString("id"))){
							logger.debug("They have the same id - We need to merge them");
							redis.zrem(getKey(angelId), currentSegment.toString(), remAr->handleSegmentRemoved(angelId, segmentInterval,currentSegment,remAr));
						}
						else{
							logger.debug("They have different id-> Just add the interval as a new segment");
							redis.zadd(getKey(angelId), getReadingTime(segmentInterval), segmentInterval.toString(), ar->handleSegmentAdded(segmentInterval.toString(),ar));
						}
					}
				}
			}
		}
		else
			logger.error("Problem On ZRange for {}: ",segmentInterval,zrangeAr.cause());
	}



	private void handleSegmentRemoved(final int angelId,final JsonObject segmentInterval,final JsonObject currentSegment, final AsyncResult<Long> remAr) {
		if (remAr.succeeded()){
			logger.debug("Removed Segment from Redis, as we are about to replace it with an updated one. Segment is {}",currentSegment);
			final JsonObject lastGPSInInterval = segmentInterval.getJsonObject("lastGPSInInterval");
			currentSegment.put("lastGPSInInterval", lastGPSInInterval);
			redis.zadd(getKey(angelId), getReadingTime(segmentInterval), currentSegment.toString(), ar->handleSegmentAdded(currentSegment.toString(),ar));
		}else{
			logger.error("Problem on remove Segment {}: ",currentSegment,remAr.cause());
		}
	}



	private void handleSegmentAdded(final String segment, final AsyncResult<Long> ar) {
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
