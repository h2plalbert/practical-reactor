import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import java.time.Duration
import java.util.function.Consumer

/**
 * In Reactor a Sink allows safe manual triggering of signals. We will learn more about multicasting and backpressure in
 * the next chapters.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#sinks
 * https://projectreactor.io/docs/core/release/reference/#processor-overview
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c8_Sinks : SinksBase() {
    /**
     * You need to execute operation that is submitted to legacy system which does not support Reactive API. You want to
     * avoid blocking and let subscribers subscribe to `operationCompleted` Mono, that will emit `true` once submitted
     * operation is executed by legacy system.
     */
    @Test
    fun single_shooter() {
        //todo: feel free to change code as you need
        val operationCompleted: Mono<Boolean>? = null
        submitOperation {
            doSomeWork() //don't change this line
        }

        //don't change code below
        StepVerifier.create(operationCompleted!!.timeout(Duration.ofMillis(5500))).expectNext(true).verifyComplete()
    }

    /**
     * Similar to previous exercise, you need to execute operation that is submitted to legacy system which does not
     * support Reactive API. This time you need to obtain result of `get_measures_reading()` and emit it to subscriber.
     * If measurements arrive before subscribers subscribe to `get_measures_readings()`, buffer them and emit them to
     * subscribers once they are subscribed.
     */
    @Test
    fun single_subscriber() {
        //todo: feel free to change code as you need
        val measurements: Flux<Int>? = null
        submitOperation {
            val measures_readings = get_measures_readings() //don't change this line
        }

        //don't change code below
        StepVerifier.create(
            measurements?.delaySubscription(Duration.ofSeconds(6))
        ).expectNext(0x0800, 0x0B64, 0x0504).verifyComplete()
    }

    /**
     * Same as previous exercise, but with twist that you need to emit measurements to multiple subscribers.
     * Subscribers should receive only the signals pushed through the sink after they have subscribed.
     */
    @Test
    fun it_gets_crowded() {
        //todo: feel free to change code as you need
        val measurements: Flux<Int>? = null
        submitOperation {
            val measures_readings = get_measures_readings() //don't change this line
        }

        //don't change code below
        StepVerifier.create(
            Flux.merge(
                measurements?.delaySubscription(Duration.ofSeconds(1)), measurements!!.ignoreElements()
            )
        ).expectNext(0x0800, 0x0B64, 0x0504).verifyComplete()
    }

    /**
     * By default, if all subscribers have cancelled (which basically means they have all un-subscribed), sink clears
     * its internal buffer and stops accepting new subscribers. For this exercise, you need to make sure that if all
     * subscribers have cancelled, the sink will still accept new subscribers. Change this behavior by setting the
     * `autoCancel` parameter.
     */
    @Test
    fun open_24_7() {
        //todo: set autoCancel parameter to prevent sink from closing
        val sink = Sinks.many().multicast().onBackpressureBuffer<Int>()
        val flux = sink.asFlux()

        //don't change code below
        submitOperation {
            get_measures_readings().forEach(Consumer { t: Int -> sink.tryEmitNext(t) })
            submitOperation { sink.tryEmitComplete() }
        }

        //subscriber1 subscribes, takes one element and cancels
        val sub1 = StepVerifier.create(Flux.merge(flux.take(1))).expectNext(0x0800).expectComplete().verifyLater()

        //subscriber2 subscribes, takes one element and cancels
        val sub2 = StepVerifier.create(Flux.merge(flux.take(1))).expectNext(0x0800).expectComplete().verifyLater()

        //subscriber3 subscribes after all previous subscribers have cancelled
        val sub3 = StepVerifier.create(
            flux.take(3).delaySubscription(Duration.ofSeconds(6))
        ).expectNext(0x0B64) //first measurement `0x0800` was already consumed by previous subscribers
            .expectNext(0x0504).expectComplete().verifyLater()
        sub1.verify()
        sub2.verify()
        sub3.verify()
    }

    /**
     * If you look closely, in previous exercises third subscriber was able to receive only two out of three
     * measurements. That's because used sink didn't remember history to re-emit all elements to new subscriber.
     * Modify solution from `open_24_7` so third subscriber will receive all measurements.
     */
    @Test
    fun blue_jeans() {
        //todo: enable autoCancel parameter to prevent sink from closing
        val sink = Sinks.many().multicast().onBackpressureBuffer<Int>()
        val flux = sink.asFlux()

        //don't change code below
        submitOperation {
            get_measures_readings().forEach(Consumer { t: Int -> sink.tryEmitNext(t) })
            submitOperation { sink.tryEmitComplete() }
        }

        //subscriber1 subscribes, takes one element and cancels
        val sub1 = StepVerifier.create(Flux.merge(flux.take(1))).expectNext(0x0800).expectComplete().verifyLater()

        //subscriber2 subscribes, takes one element and cancels
        val sub2 = StepVerifier.create(Flux.merge(flux.take(1))).expectNext(0x0800).expectComplete().verifyLater()

        //subscriber3 subscribes after all previous subscribers have cancelled
        val sub3 = StepVerifier.create(
            flux.take(3).delaySubscription(Duration.ofSeconds(6))
        ).expectNext(0x0800).expectNext(0x0B64).expectNext(0x0504).expectComplete().verifyLater()
        sub1.verify()
        sub2.verify()
        sub3.verify()
    }

    /**
     * There is a bug in the code below. May multiple producer threads concurrently generate data on the sink?
     * If yes, how? Find out and fix it.
     */
    @Test
    fun emit_failure() {
        //todo: feel free to change code as you need
        val sink = Sinks.many().replay().all<Int>()
        for (i in 1..50) {
            Thread { sink.tryEmitNext(i) }.start()
        }

        //don't change code below
        StepVerifier.create(sink.asFlux().doOnNext { x: Int? -> println(x) }.take(50)).expectNextCount(50)
            .verifyComplete()
    }
}
