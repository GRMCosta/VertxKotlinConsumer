package com.grmcosta

import io.vertx.core.AbstractVerticle
import io.vertx.ext.amqp.AmqpClient
import io.vertx.ext.amqp.AmqpClientOptions
import io.vertx.ext.amqp.AmqpConnection
import io.vertx.ext.amqp.AmqpReceiver
import org.slf4j.LoggerFactory


class ConsumerVerticle : AbstractVerticle() {

    private val LOGGER = LoggerFactory.getLogger(Consumer::class.java)

    private var client: AmqpClient? = null

    @Throws(Exception::class)
    override fun start() {
        val options: AmqpClientOptions = AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
        // Create a client using its own internal Vert.x instance.
        //AmqpClient client = AmqpClient.create(options);

        // USe an explicit Vert.x instance.
        client = AmqpClient.create(vertx, options)
        client?.connect { ar ->
            if (ar.failed()) {
                LOGGER.info("Unable to connect to the broker")
            } else {
                LOGGER.info("Connection succeeded")
                val connection: AmqpConnection = ar.result()
                connection.createReceiver("my-queue") { done ->
                    if (done.failed()) {
                        LOGGER.info("Unable to create a sender")
                    } else {
                        val receiver: AmqpReceiver = done.result()
                        receiver
                                .exceptionHandler { t -> LOGGER.error(t.message) }
                                .handler { msg -> LOGGER.info(msg.bodyAsString()) }
                    }
                }
            }
        }
        super.start()
    }

    @Throws(Exception::class)
    override fun stop() {
        client?.close { close ->
            if (close.succeeded()) {
                LOGGER.info("AmqpClient closed successfully")
            }
        }
        super.stop()
    }
}