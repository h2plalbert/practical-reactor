import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.reactivestreams.Subscription
import reactor.core.Exceptions
import reactor.core.publisher.BaseSubscriber
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.test.StepVerifier
import reactor.test.StepVerifierOptions
import java.time.Duration
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Backpressure is a mechanism that allows a consumer to signal to a producer that it is ready receive data.
 * This is important because the producer may be sending data faster than the consumer can process it, and can overwhelm consumer.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactive.backpressure
 * https://projectreactor.io/docs/core/release/reference/#_on_backpressure_and_ways_to_reshape_requests
 * https://projectreactor.io/docs/core/release/reference/#_operators_that_change_the_demand_from_downstream
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_asynchronous_but_single_threaded_push
 * https://projectreactor.io/docs/core/release/reference/#_a_hybrid_pushpull_model
 * https://projectreactor.io/docs/core/release/reference/#_an_alternative_to_lambdas_basesubscriber
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c10_Backpressure : BackpressureBase() {
    /**
     * In this exercise subscriber (test) will request several messages from the message stream.
     * Hook to the requests and record them to the `requests` list.
     */
    @Test
    fun request_and_demand() {
        val requests = CopyOnWriteArrayList<Long>()
        val messageStream = messageStream1() //todo: change this line only
        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
            .expectSubscription()
            .thenRequest(1)
            .then { pub1.next("msg#1") }
            .thenRequest(3)
            .then { pub1.next("msg#2", "msg#3") }
            .then { pub1.complete() }
            .expectNext("msg#1", "msg#2", "msg#3")
            .verifyComplete()
        Assertions.assertEquals(listOf(1L, 3L), requests)
    }

    /**
     * Adjust previous solution in such a way that you limit rate of requests. Number of requested messages stays the
     * same, but each request should be limited to 1 message.
     */
    @Test
    fun limited_demand() {
        val requests = CopyOnWriteArrayList<Long>()
        val messageStream = messageStream2() //todo: do your changes here
        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
            .expectSubscription()
            .thenRequest(1)
            .then { pub2.next("msg#1") }
            .thenRequest(3)
            .then { pub2.next("msg#2", "msg#3") }
            .then { pub2.complete() }
            .expectNext("msg#1", "msg#2", "msg#3")
            .verifyComplete()
        Assertions.assertEquals(listOf(1L, 1L, 1L, 1L), requests)
    }

    /**
     * Finish the implementation of the `uuidGenerator` so it exactly requested amount of UUIDs. Or better said, it
     * should respect the backpressure of the consumer.
     */
    @Test
    fun uuid_generator() {
        val uuidGenerator = Flux.create { sink: FluxSink<UUID?>? -> }
        StepVerifier.create(uuidGenerator
            .doOnNext { x: UUID? -> println(x) }
            .timeout(Duration.ofSeconds(1))
            .onErrorResume(TimeoutException::class.java) { e: TimeoutException? -> Flux.empty() },
            StepVerifierOptions.create().initialRequest(0)
        )
            .expectSubscription()
            .thenRequest(10)
            .expectNextCount(10)
            .thenCancel()
            .verify()
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure.
     * In case that publisher produces more messages than subscriber is able to consume, raise an error.
     */
    @Test
    fun pressure_is_too_much() {
        val messageStream = messageStream3() //todo: change this line only
        StepVerifier.create(
            messageStream, StepVerifierOptions.create()
                .initialRequest(0)
        )
            .expectSubscription()
            .thenRequest(3)
            .then { pub3.next("A", "B", "C", "D") }
            .expectNext("A", "B", "C")
            .expectErrorMatches { t: Throwable? -> Exceptions.isOverflow(t) }
            .verify()
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure. In case that publisher
     * produces more messages than subscriber is able to consume, buffer them for later consumption without raising an
     * error.
     */
    @Test
    fun u_wont_brake_me() {
        val messageStream = messageStream4() //todo: change this line only
        StepVerifier.create(
            messageStream, StepVerifierOptions.create()
                .initialRequest(0)
        )
            .expectSubscription()
            .thenRequest(3)
            .then { pub4.next("A", "B", "C", "D") }
            .expectNext("A", "B", "C")
            .then { pub4.complete() }
            .thenAwait()
            .thenRequest(1)
            .expectNext("D")
            .verifyComplete()
    }

    /**
     * We saw how to react to request demand from producer side. In this part we are going to control demand from
     * consumer side by implementing BaseSubscriber directly.
     * Finish implementation of base subscriber (consumer of messages) with following objectives:
     * - once there is subscription, you should request exactly 10 messages from publisher
     * - once you received 10 messages, you should cancel any further requests from publisher.
     * Producer respects backpressure.
     */
    @Test
    @Throws(InterruptedException::class)
    fun subscriber() {
        val lockRef = AtomicReference(CountDownLatch(1))
        val count = AtomicInteger(0)
        val sub = AtomicReference<Subscription>()
        remoteMessageProducer()
            .doOnCancel { lockRef.get().countDown() }
            .subscribeWith(object : BaseSubscriber<String?>() {
                //todo: do your changes only within BaseSubscriber class implementation
                override fun hookOnSubscribe(subscription: Subscription) {
                    sub.set(subscription)
                }

                override fun hookOnNext(s: String?) {
                    println(s)
                    count.incrementAndGet()
                } //-----------------------------------------------------
            })
        lockRef.get().await()
        Assertions.assertEquals(10, count.get())
    }
}
