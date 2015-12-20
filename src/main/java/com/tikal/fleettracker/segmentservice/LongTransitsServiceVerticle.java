package com.tikal.fleettracker.segmentservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

public class LongTransitsServiceVerticle extends AbstractVerticle {

	@Override
	public void start() {
		vertx.deployVerticle(new LongTransitsFinderVerticle(),new DeploymentOptions().setConfig(config()));
		vertx.deployVerticle(new SegmentMongoPersistVerticle(),new DeploymentOptions().setConfig(config()));
	}
	

}
