import reactor.core.publisher.Flux
import reactor.core.publisher.SynchronousSink
import reactor.test.publisher.TestPublisher
import java.util.*

/**
 * @author Stefan Dragisic
 */
open class BackpressureBase {
    @JvmField
    var pub1 = TestPublisher.create<String>()
    fun messageStream1(): Flux<String> {
        return pub1.flux()
    }

    @JvmField
    var pub2 = TestPublisher.create<String>()
    fun messageStream2(): Flux<String> {
        return pub2.flux()
    }

    @JvmField
    var pub3 = TestPublisher.createNoncompliant<String>(TestPublisher.Violation.REQUEST_OVERFLOW)
    fun messageStream3(): Flux<String> {
        return pub3.flux()
    }

    @JvmField
    var pub4 = TestPublisher.createNoncompliant<String>(TestPublisher.Violation.REQUEST_OVERFLOW)
    fun messageStream4(): Flux<String> {
        return pub4.flux()
    }

    fun remoteMessageProducer(): Flux<String?> {
        return Flux.generate { s: SynchronousSink<String?> -> s.next("MESSAGE#" + UUID.randomUUID()) }
    }
}
