package org.dfpl.chronograph.chronoweb;

import java.net.Inet4Address;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.dfpl.chronograph.khronos.memory.manipulation.ChronoGraph;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class Server extends AbstractVerticle {

	public static Logger logger;
	public static int port = 80;
	public ChronoGraph graph;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		super.start(startPromise);

		final HttpServer server = vertx.createHttpServer();
		final Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		graph = new ChronoGraph();

		StaticRouter.registerAddVertexRouter(router, graph);

		server.requestHandler(router).listen(80);
		logger.info(
				"Chronoweb runs at http://" + Inet4Address.getLocalHost().getHostAddress() + ":" + port + "/chronoweb");
	}

	public static void setLogger() {
		Configurator.setRootLevel(Level.OFF);
		Configurator.setLevel(Server.class, Level.DEBUG);
		logger = LogManager.getLogger(Server.class);
	}

	public static void main(String[] args) {
		setLogger();
		Vertx.vertx().deployVerticle(new Server());
	}
}
