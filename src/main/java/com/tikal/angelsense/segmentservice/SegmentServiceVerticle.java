package com.tikal.angelsense.segmentservice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;

public class SegmentServiceVerticle extends AbstractVerticle {

	@Override
	public void start() {
		vertx.deployVerticle(new SegmentFinderServiceVerticle(),new DeploymentOptions().setConfig(config()));
		vertx.deployVerticle(new SegmentPersistVerticle(),new DeploymentOptions().setConfig(config()));
	}
	

}
