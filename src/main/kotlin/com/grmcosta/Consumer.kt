package com.grmcosta

import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import org.slf4j.LoggerFactory
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Observes
import javax.inject.Inject

@ApplicationScoped
class Consumer {

    private val LOGGER = LoggerFactory.getLogger(Consumer::class.java)

    @Inject
    private val vertx: Vertx? = null

    private var deploymentId: String? = null


    fun onStart(@Observes ev: StartupEvent?) {
        LOGGER.info("onStart")
        startAmqpConnection()
    }

    fun onStop(@Observes ev: ShutdownEvent?) {
        LOGGER.info("onStop")
        stopAmqpConnection()
    }

    private fun startAmqpConnection() {
        val producerVerticle = ConsumerVerticle()
        vertx?.deployVerticle(producerVerticle) { handler: AsyncResult<String?> ->
            if (handler.succeeded()) {
                deploymentId = handler.result()
            }
        }
    }

    private fun stopAmqpConnection() {
        vertx?.undeploy(deploymentId) { handler: AsyncResult<Void?> ->
            if (handler.succeeded()) {
                vertx.close()
            }
        }
    }
}
