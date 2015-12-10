package com.tikal.angelsense.segmentservice;

import java.util.Arrays;
import java.util.List;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.BridgeEventType;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

public class SegmentFinderServiceVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SegmentFinderServiceVerticle.class);
	private MongoClient mongoClient;
	private String collectionName;

	@Override
	public void start() {
		mongoClient = MongoClient.createShared(vertx, config().getJsonObject("mongoConfig"));
		collectionName = config().getJsonObject("mongoConfig").getString("segments_col_name");

		final Router router = Router.router(vertx);
		router.route(HttpMethod.GET, "/segments/angel/:angelId").handler(this::handleQuery);
		// Allow outbound traffic to the segments-feed address
		final BridgeOptions options = new BridgeOptions().addOutboundPermitted(new PermittedOptions().setAddressRegex("segments-feed.*"));
		router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options, this::handleBridgeEvent));
		router.route().handler(StaticHandler.create());
		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http-port"));

		logger.info("Started the HTTP server...");

	}

	private void handleBridgeEvent(final BridgeEvent event) {
		if (event.type() == BridgeEventType.SOCKET_CREATED)
			logger.info("A socket was created");
		event.complete(true);
	}

	private void handleQuery(final RoutingContext routingContext) {
		final String angelId = routingContext.request().getParam("angelId");
		if (angelId == null)
			routingContext.response().setStatusCode(400).setStatusMessage("angelId is missing").end();
		else {
			final String start = routingContext.request().params().get("start");
			final String stop = routingContext.request().params().get("stop");
			final JsonObject datePart = new JsonObject().put("startTime",	new JsonObject().put("$gte", Long.valueOf(start)).put("$lte", Long.valueOf(stop)));
			final JsonObject angelIdPart = new JsonObject().put("angelId",Integer.valueOf(angelId));
			final JsonObject query = new JsonObject().put("$and", new JsonArray(Arrays.asList(angelIdPart,datePart)));
			logger.debug("query is : {}",query);
			final FindOptions findOptions = new FindOptions(new JsonObject().put("sort", new JsonObject().put("startTime", -1)));
			mongoClient.findWithOptions(collectionName, query,findOptions , ar -> handleMongoQuery(ar, routingContext));
		}
	}
	
	private void handleMongoQuery(final AsyncResult<List<JsonObject>> ar, final RoutingContext routingContext) {
		if (ar.succeeded()) {
			routingContext.response().end(Buffer.factory.buffer(ar.result().toString()));
		} else {
			logger.error("Failed to run Mongo query: ", ar.cause());
			routingContext.response().setStatusCode(500).setStatusMessage(ar.cause().getMessage()).end();
		}
	}

}
