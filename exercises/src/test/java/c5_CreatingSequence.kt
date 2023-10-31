import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.publisher.Mono
import reactor.core.publisher.SynchronousSink
import reactor.test.StepVerifier
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream

/**
 * In this chapter we are going to cover fundamentals of how to create a sequence. At the end of this
 * chapter we will tackle more complex methods like generate, create, push, and we will meet them again in following
 * chapters like Sinks and Backpressure.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.create
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_simple_ways_to_create_a_flux_or_mono_and_subscribe_to_it
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c5_CreatingSequence {
    /**
     * Emit value that you already have.
     */
    @Test
    fun value_I_already_have_mono() {
        val valueIAlreadyHave = "value"
        val valueIAlreadyHaveMono: Mono<String>? = null //todo: change this line only
        StepVerifier.create(valueIAlreadyHaveMono)
            .expectNext("value")
            .verifyComplete()
    }

    /**
     * Emit potentially null value that you already have.
     */
    @Test
    fun potentially_null_mono() {
        val potentiallyNull: String? = null
        val potentiallyNullMono: Mono<String>? = null //todo change this line only
        StepVerifier.create(potentiallyNullMono)
            .verifyComplete()
    }

    /**
     * Emit value from a optional.
     */
    @Test
    fun optional_value() {
        val optionalValue = Optional.of("optional")
        val optionalMono: Mono<String>? = null //todo: change this line only
        StepVerifier.create(optionalMono)
            .expectNext("optional")
            .verifyComplete()
    }

    /**
     * Convert callable task to Mono.
     */
    @Test
    fun callable_counter() {
        val callableCounter = AtomicInteger(0)
        val callable = Callable {
            println("You are incrementing a counter via Callable!")
            callableCounter.incrementAndGet()
        }
        val callableCounterMono: Mono<Int>? = null //todo: change this line only
        StepVerifier.create(callableCounterMono!!.repeat(2))
            .expectNext(1, 2, 3)
            .verifyComplete()
    }

    /**
     * Convert Future task to Mono.
     */
    @Test
    fun future_counter() {
        val futureCounter = AtomicInteger(0)
        val completableFuture = CompletableFuture.supplyAsync {
            println("You are incrementing a counter via Future!")
            futureCounter.incrementAndGet()
        }
        val futureCounterMono: Mono<Int>? = null //todo: change this line only
        StepVerifier.create(futureCounterMono)
            .expectNext(1)
            .verifyComplete()
    }

    /**
     * Convert Runnable task to Mono.
     */
    @Test
    fun runnable_counter() {
        val runnableCounter = AtomicInteger(0)
        val runnable = Runnable {
            runnableCounter.incrementAndGet()
            println("You are incrementing a counter via Runnable!")
        }
        val runnableMono: Mono<Int>? = null //todo: change this line only
        StepVerifier.create(runnableMono!!.repeat(2))
            .verifyComplete()
        Assertions.assertEquals(3, runnableCounter.get())
    }

    /**
     * Create Mono that emits no value but completes successfully.
     */
    @Test
    fun acknowledged() {
        val acknowledged: Mono<String>? = null //todo: change this line only
        StepVerifier.create(acknowledged)
            .verifyComplete()
    }

    /**
     * Create Mono that emits no value and never completes.
     */
    @Test
    fun seen() {
        val seen: Mono<String>? = null //todo: change this line only
        StepVerifier.create(seen!!.timeout(Duration.ofSeconds(5)))
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(4))
            .verifyTimeout(Duration.ofSeconds(5))
    }

    /**
     * Create Mono that completes exceptionally with exception `IllegalStateException`.
     */
    @Test
    fun trouble_maker() {
        val trouble: Mono<String>? = null //todo: change this line
        StepVerifier.create(trouble)
            .expectError(IllegalStateException::class.java)
            .verify()
    }

    /**
     * Create Flux that will emit all values from the array.
     */
    @Test
    fun from_array() {
        val array = arrayOf(1, 2, 3, 4, 5)
        val arrayFlux: Flux<Int>? = null //todo: change this line only
        StepVerifier.create(arrayFlux)
            .expectNext(1, 2, 3, 4, 5)
            .verifyComplete()
    }

    /**
     * Create Flux that will emit all values from the list.
     */
    @Test
    fun from_list() {
        val list: List<String> = mutableListOf("1", "2", "3", "4", "5")
        val listFlux: Flux<String>? = null //todo: change this line only
        StepVerifier.create(listFlux)
            .expectNext("1", "2", "3", "4", "5")
            .verifyComplete()
    }

    /**
     * Create Flux that will emit all values from the stream.
     */
    @Test
    fun from_stream() {
        val stream = Stream.of("5", "6", "7", "8", "9")
        val streamFlux: Flux<String>? = null //todo: change this line only
        StepVerifier.create(streamFlux)
            .expectNext("5", "6", "7", "8", "9")
            .verifyComplete()
    }

    /**
     * Create Flux that emits number incrementing numbers at interval of 1 second.
     */
    @Test
    fun interval() {
        val interval: Flux<Long>? = null //todo: change this line only
        println("Interval: ")
        StepVerifier.create(interval!!.take(3).doOnNext { x: Long? -> println(x) })
            .expectSubscription()
            .expectNext(0L)
            .expectNoEvent(Duration.ofMillis(900))
            .expectNext(1L)
            .expectNoEvent(Duration.ofMillis(900))
            .expectNext(2L)
            .verifyComplete()
    }

    /**
     * Create Flux that emits range of integers from [-5,5].
     */
    @Test
    fun range() {
        val range: Flux<Int>? = null //todo: change this line only
        println("Range: ")
        StepVerifier.create(range!!.doOnNext { x: Int? -> println(x) })
            .expectNext(-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5)
            .verifyComplete()
    }

    /**
     * Create Callable that increments the counter and returns the counter value, and then use `repeat()` operator to create Flux that emits
     * values from 0 to 10.
     */
    @Test
    fun repeat() {
        val counter = AtomicInteger(0)
        val repeated: Flux<Int>? = null //todo: change this line
        println("Repeat: ")
        StepVerifier.create(repeated!!.doOnNext { x: Int? -> println(x) })
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete()
    }

    /**
     * Following example is just a basic usage of `generate,`create`,`push` sinks. We will learn how to use them in a
     * more complex scenarios when we tackle backpressure.
     *
     * Answer:
     * - What is difference between `generate` and `create`?
     * - What is difference between `create` and `push`?
     */
    @Test
    fun generate_programmatically() {
        val generateFlux = Flux.generate { sink: SynchronousSink<Int?>? -> }

        //------------------------------------------------------
        val createFlux = Flux.create { sink: FluxSink<Int?>? -> }

        //------------------------------------------------------
        val pushFlux = Flux.push { sink: FluxSink<Int?>? -> }
        StepVerifier.create(generateFlux)
            .expectNext(0, 1, 2, 3, 4, 5)
            .verifyComplete()
        StepVerifier.create(createFlux)
            .expectNext(0, 1, 2, 3, 4, 5)
            .verifyComplete()
        StepVerifier.create(pushFlux)
            .expectNext(0, 1, 2, 3, 4, 5)
            .verifyComplete()
    }

    /**
     * Something is wrong with the following code. Find the bug and fix it so test passes.
     */
    @Test
    fun multi_threaded_producer() {
        //todo: find a bug and fix it!
        val producer = Flux.push { sink: FluxSink<Int?> ->
            for (i in 0..99) {
                Thread { sink.next(i) }.start() //don't change this line!
            }
        }

        //do not change code below
        StepVerifier.create(producer
            .doOnNext { x: Int? -> println(x) }
            .take(100))
            .expectNextCount(100)
            .verifyComplete()
    }
}
