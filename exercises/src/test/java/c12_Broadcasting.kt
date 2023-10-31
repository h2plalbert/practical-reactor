import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.util.concurrent.CopyOnWriteArrayList

/**
 * In this chapter we will learn difference between hot and cold publishers,
 * how to split a publisher into multiple and how to keep history so late subscriber don't miss any updates.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactor.hotCold
 * https://projectreactor.io/docs/core/release/reference/#which.multicasting
 * https://projectreactor.io/docs/core/release/reference/#advanced-broadcast-multiple-subscribers-connectableflux
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c12_Broadcasting : BroadcastingBase() {
    /**
     * Split incoming message stream into two streams, one contain user that sent message and second that contains
     * message payload.
     */
    @Test
    @Throws(InterruptedException::class)
    fun sharing_is_caring() {
        val messages = messageStream() //todo: do your changes here

        //don't change code below
        val userStream = messages.map { m: Message -> m.user }
        val payloadStream = messages.map { m: Message -> m.payload }
        val metaData = CopyOnWriteArrayList<String>()
        val payload = CopyOnWriteArrayList<String>()
        userStream.doOnNext { n: String -> println("User: $n") }.subscribe { e: String -> metaData.add(e) }
        payloadStream.doOnNext { n: String -> println("Payload: $n") }.subscribe { e: String -> payload.add(e) }
        Thread.sleep(3000)
        Assertions.assertEquals(mutableListOf("user#0", "user#1", "user#2", "user#3", "user#4"), metaData)
        Assertions.assertEquals(
            mutableListOf("payload#0", "payload#1", "payload#2", "payload#3", "payload#4"),
            payload
        )
    }

    /**
     * Since two subscribers are interested in the updates, which are coming from same source, convert `updates` stream
     * to from cold to hot source.
     * Answer: What is the difference between hot and cold publisher? Why does won't .share() work in this case?
     */
    @Test
    fun hot_vs_cold() {
        val updates = systemUpdates() //todo: do your changes here

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext { n: String -> println("subscriber 1 got: $n") })
            .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
            .verifyComplete()

        //subscriber 2
        StepVerifier.create(updates.take(4).doOnNext { n: String -> println("subscriber 2 got: $n") })
            .expectNext("DISK_SPACE_LOW", "OOM_DETECTED", "CRASHED", "UNKNOWN")
            .verifyComplete()
    }

    /**
     * In previous exercise second subscriber subscribed to update later, and it missed some updates. Adapt previous
     * solution so second subscriber will get all updates, even the one's that were broadcaster before its
     * subscription.
     */
    @Test
    fun history_lesson() {
        val updates = systemUpdates() //todo: do your changes here

        //subscriber 1
        StepVerifier.create(updates.take(3).doOnNext { n: String -> println("subscriber 1 got: $n") })
            .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY")
            .verifyComplete()

        //subscriber 2
        StepVerifier.create(updates.take(5).doOnNext { n: String -> println("subscriber 2 got: $n") })
            .expectNext("RESTARTED", "UNHEALTHY", "HEALTHY", "DISK_SPACE_LOW", "OOM_DETECTED")
            .verifyComplete()
    }
}
