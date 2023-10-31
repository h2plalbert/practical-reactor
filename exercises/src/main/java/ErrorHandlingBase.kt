import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Stefan Dragisic
 */
open class ErrorHandlingBase {
    @JvmField
    var errorReported = AtomicBoolean(false)
    var counter = AtomicInteger(3)
    var gate = AtomicBoolean(false)
    var scheduled = AtomicBoolean(false)
    var invokedCounter = AtomicInteger(0)
    var pollCounter = AtomicInteger(0)
    fun probeHeartBeatSignal(): Flux<String> {
        return Flux.concat(
            Flux.just("keep-alive"), Flux.just("keep-alive"), Flux.just("keep-alive"), Flux.never()
        ).delayElements(Duration.ofSeconds(1)).doOnNext { n: String -> println("Service: $n") }
    }

    val currentUser: Mono<String>
        get() = Mono.error(IllegalAccessError("No active session, user not found!"))

    fun messageNode(): Flux<String> {
        return Flux.just("0x1", "0x2").concatWith(Flux.error(RuntimeException("Service shutdown unexpectedly!")))
    }

    fun backupMessageNode(): Flux<String> {
        return Flux.just("0x3", "0x4")
    }

    fun errorReportService(error: Throwable): Mono<Void> {
        return Mono.fromRunnable {
            errorReported.set(true)
            println("Thank you for reporting this error!")
            println("Error reported: " + error.message)
        }
    }

    fun taskQueue(): Flux<Task> {
        return Flux.just(Task(), Task())
    }

    val filesContent: Flux<Mono<String>>
        get() = Flux.just("file1.txt", "file2.txt", "file3.txt").doOnNext { n: String -> println("Reading file: $n") }
            .map { n: String ->
                Mono.fromCallable {
                    if (n == "file2.txt") {
                        throw RuntimeException("file2.txt is broken")
                    }
                    "$n content"
                }
            }.doOnError { e: Throwable -> println("Error reading file: " + e.message) }

    fun temperatureSensor(): Mono<Int> {
        return Mono.fromCallable {
            if (counter.decrementAndGet() == 0) {
                return@fromCallable 34
            } else {
                throw RuntimeException("Sensor reading failed!")
            }
        }
    }

    fun establishConnection(): Mono<String> {
        return Mono.fromCallable {
            invokedCounter.incrementAndGet()
            if (invokedCounter.get() > 3) {
                println("You are not allowed to connect more than 3 times!")
                throw RuntimeException("You are not allowed to connect more than 3 times!")
            }
            println("Establishing connection...")
            if (gate.get()) {
                return@fromCallable "connection_established"
            } else {
                if (!scheduled.get()) {
                    scheduled.set(true)
                    Executors.newScheduledThreadPool(1).schedule({ gate.set(true) }, 5, TimeUnit.SECONDS)
                }
                throw RuntimeException("Sensor reading failed!")
            }
        }
    }

    fun nodeAlerts(): Mono<String?> {
        return Mono.fromCallable {
            println("------------------------------------------------------")
            println("SELECT * FROM alerts LIMIT 1")
            if (pollCounter.incrementAndGet() == 5) {
                println("New alert: Node1 is low on disk space!")
                return@fromCallable "node1:low_disk_space"
            } else if (pollCounter.get() == 7) {
                println("New alert: Node1 is down!")
                return@fromCallable "node1:down"
            } else {
                println("No alerts found!")
                return@fromCallable null
            }
        }.subscribeOn(Schedulers.boundedElastic())
    }

    class Task {
        @JvmField
        var executedSuccessfully = AtomicBoolean(false)

        @JvmField
        var executedExceptionally = AtomicBoolean(false)
        fun execute(): Mono<Void> {
            return Mono.fromRunnable {
                println("Executing task...")
                if (counter.incrementAndGet() == 1) {
                    throw RuntimeException("Task failed!")
                } else {
                    println("Task executed successfully...")
                }
            }
        }

        fun commit(): Mono<Void> {
            return Mono.fromRunnable {
                executedSuccessfully.set(true)
                println("Committing task...")
            }
        }

        fun rollback(error: Throwable): Mono<Void> {
            return Mono.fromRunnable {
                executedExceptionally.set(true)
                println("Rollback task... Cause: " + error.message)
            }
        }

        companion object {
            var counter = AtomicInteger(0)
        }
    }
}
