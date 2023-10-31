import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

/**
 * This chapter will introduce you to the basics of Reactor.
 * You will learn how to retrieve result from Mono and Flux
 * in different ways.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#intro-reactive
 * https://projectreactor.io/docs/core/release/reference/#reactive.subscribe
 * https://projectreactor.io/docs/core/release/reference/#_subscribe_method_examples
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c1_Introduction : IntroductionBase() {
    /**
     * Every journey starts with Hello World!
     * As you may know, Mono represents asynchronous result of 0-1 element.
     * Retrieve result from this Mono by blocking indefinitely or until a next signal is received.
     */
    @Test
    fun hello_world() {
        val serviceResult = hello_world_service()
        val result: String? = null //todo: change this line only
        Assertions.assertEquals("Hello World!", result)
    }

    /**
     * Retrieving result should last for a limited time amount of time, or you might get in trouble.
     * Try retrieving result from service by blocking for maximum of 1 second or until a next signal is received.
     */
    @Test
    fun unresponsive_service() {
        val exception: Exception = Assertions.assertThrows(
            IllegalStateException::class.java
        ) {
            val serviceResult = unresponsiveService()
            val result: String? = null //todo: change this line only
        }
        val expectedMessage = "Timeout on blocking read for 1"
        val actualMessage = exception.message
        Assertions.assertTrue(actualMessage!!.contains(expectedMessage))
    }

    /**
     * Services are unpredictable, they might and might not return a result and no one likes nasty NPE's.
     * Retrieve result from the service as optional object.
     */
    @Test
    fun empty_service() {
        val serviceResult = emptyService()
        val optionalServiceResult: Optional<String>? = null //todo: change this line only
        Assertions.assertTrue(optionalServiceResult!!.isEmpty)
        Assertions.assertTrue(emptyServiceIsCalled.get())
    }

    /**
     * Many services return more than one result and best services supports streaming!
     * It's time to introduce Flux, an Asynchronous Sequence of 0-N Items.
     *
     * Service we are calling returns multiple items, but we are interested only in the first one.
     * Retrieve first item from this Flux by blocking indefinitely until a first item is received.
     */
    @Test
    fun multi_result_service() {
        val serviceResult = multiResultService()
        val result = serviceResult.toString() //todo: change this line only
        Assertions.assertEquals("valid result", result)
    }

    /**
     * We have the service that returns list of fortune top five companies.
     * Collect companies emitted by this service into a list.
     * Retrieve results by blocking.
     */
    @Test
    fun fortune_top_five() {
        val serviceResult = fortuneTop5()
        val results = emptyList<String>() //todo: change this line only
        Assertions.assertEquals(
            mutableListOf("Walmart", "Amazon", "Apple", "CVS Health", "UnitedHealth Group"),
            results
        )
        Assertions.assertTrue(fortuneTop5ServiceIsCalled.get())
    }

    /***
     * "I Used an Operator on my Flux, but it Doesn’t Seem to Apply. What Gives?"
     *
     * Previously we retrieved result by blocking on a Mono/Flux.
     * That really beats whole purpose of non-blocking and asynchronous library like Reactor.
     * Blocking operators are usually used for testing or when there is no way around, and
     * you need to go back to synchronous world.
     *
     * Fix this test without using any blocking operator.
     * Change only marked line!
     */
    @Test
    @Throws(InterruptedException::class)
    fun nothing_happens_until_you_() {
        val companyList = CopyOnWriteArrayList<String>()
        val serviceResult = fortuneTop5()
        serviceResult
            .doOnNext { e: String -> companyList.add(e) } //todo: add an operator here, don't use any blocking operator!
        Thread.sleep(1000) //bonus: can you explain why this line is needed?
        Assertions.assertEquals(
            mutableListOf("Walmart", "Amazon", "Apple", "CVS Health", "UnitedHealth Group"),
            companyList
        )
    }

    /***
     * If you finished previous task, this one should be a breeze.
     *
     * Upgrade previously used solution, so that it:
     * - adds each emitted item to `companyList`
     * - does nothing if error occurs
     * - sets `serviceCallCompleted` to `true` once service call is completed.
     *
     * Don't use doOnNext, doOnError, doOnComplete hooks.
     */
    @Test
    @Throws(InterruptedException::class)
    fun leaving_blocking_world_behind() {
        val serviceCallCompleted = AtomicReference(false)
        val companyList = CopyOnWriteArrayList<String>()
        fortuneTop5() //todo: change this line only
        Thread.sleep(1000)
        Assertions.assertTrue(serviceCallCompleted.get())
        Assertions.assertEquals(
            mutableListOf("Walmart", "Amazon", "Apple", "CVS Health", "UnitedHealth Group"),
            companyList
        )
    }
}
