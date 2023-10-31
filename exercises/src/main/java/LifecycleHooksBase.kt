import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom

/**
 * @author Stefan Dragisic
 */
open class LifecycleHooksBase {
    fun room_temperature_service(): Flux<Int> {
        return Flux.interval(Duration.ofMillis(100), Duration.ofMillis(100))
            .take(20)
            .map { i: Long? -> ThreadLocalRandom.current().nextInt(10, 30) }
    }
}
