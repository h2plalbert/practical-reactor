import org.reactivestreams.Subscription
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Stefan Dragisic
 */
open class CombiningPublishersBase {
    @JvmField
    var taskCounter = AtomicInteger(0)
    @JvmField
    var localCacheCalled = AtomicBoolean(false)
    @JvmField
    var consumedSpamCounter = AtomicInteger(0)
    @JvmField
    var fileOpened = AtomicBoolean(false)
    @JvmField
    var writtenToFile = AtomicBoolean(false)
    @JvmField
    var fileClosed = AtomicBoolean(false)
    @JvmField
    var committedTasksCounter = AtomicInteger(0)
    val currentUser: Mono<String>
        get() = Mono.just("user123")

    fun getUserEmail(user: String): Mono<String> {
        return Mono.fromSupplier { "$user@gmail.com" }
    }

    fun taskExecutor(): Flux<Mono<Void>> {
        return Flux.range(1, 10) //.delayElements(Duration.ofMillis(250))
            .map { i: Int ->
                Mono.fromRunnable<Void> {
                    println("Executing task: #$i")
                    taskCounter.incrementAndGet()
                }.subscribeOn(Schedulers.parallel())
            }
    }

    fun streamingService(): Mono<Flux<Message>> {
        return Mono.just(Flux.range(1, 10)
            .delayElements(Duration.ofMillis(250))
            .map { i: Int -> Message("chunk:$i", UUID.randomUUID().toString()) }
            .doOnNext { msg: Message -> println("-> msg#" + msg.metaData) }
            .doOnSubscribe { s: Subscription? -> println("Streaming started...") }
            .delaySubscription(Duration.ofMillis(750))
            .doOnComplete { println("Streaming finished!") })
            .doOnSubscribe { s: Subscription? -> println("Connecting to the service...") }
    }

    fun numberService1(): Flux<Int> {
        return Flux.range(1, 3).doOnNext { x: Int? -> println(x) }
    }

    fun numberService2(): Flux<Int> {
        return Flux.range(4, 4).doOnNext { x: Int? -> println(x) }
    }

    fun listAllUsers(): Flux<String> {
        return Flux.range(1, 10)
            .map { i: Int -> "user$i" }
    }

    val stocksLocalCache: Flux<String?>
        get() = Flux.defer {
            println("(LocalCache) No stocks found in local cache!")
            localCacheCalled.set(true)
            Flux.empty()
        }
    val stocksRest: Flux<String>
        get() = Flux.range(10, 6)
            .map { i: Int -> "$i$" }
            .doOnNext { n: String -> println("(REST) Got stock, price: $n") }
            .delaySubscription(Duration.ofMillis(100))
    val stocksGrpc: Flux<String>
        get() = Flux.range(1, 5)
            .map { i: Int -> "$i$" }
            .doOnNext { n: String -> println("(GRPC) Got stock, price: $n") }
            .delaySubscription(Duration.ofMillis(30))

    fun mailBoxPrimary(): Flux<Message> {
        return Flux.range(1, 3)
            .map { i: Int -> Message("spam", "0x$i") }
            .doOnNext { n: Message -> println("Message[spam, 0x" + n.payload + "]") }
            .doOnNext { n: Message? -> consumedSpamCounter.incrementAndGet() }
    }

    fun mailBoxSecondary(): Flux<Message> {
        return Flux.range(1, 2)
            .map { i: Int? -> Message("job-offer", "please join as in google!") }
            .doOnNext { n: Message? -> println("Message[job-offer, please join as in google!]") }
    }

    fun userSearchInput(): Flux<String> {
        return Flux.just("r", "re", "rea", "reac", "reac", "react", "reacto", "reactor")
            .concatWith(Flux.just("reactive").delaySubscription(Duration.ofMillis(500)))
            .doOnNext { n: String -> println("Typed: $n") }
    }

    fun autoComplete(word: String): Mono<String> {
        return Mono.just("$word project")
            .doOnNext { n: String -> println("Suggestion: $n") }
            .delaySubscription(Duration.ofMillis(100))
    }

    fun openFile(): Mono<Void> {
        return Mono.fromRunnable<Void> {
            fileOpened.set(true)
            println("Opening file...")
        }.delaySubscription(Duration.ofMillis(100))
    }

    fun writeToFile(content: String): Mono<Void> {
        return Mono.fromRunnable<Void> {
            writtenToFile.set(true)
            println("Writing: $content")
        }.delaySubscription(Duration.ofMillis(1000))
    }

    fun readFile(): Flux<String> {
        return Flux.defer {
            if (fileOpened.get()) {
                return@defer Flux.just<String>("0x1", "0x2", "0x3")
            } else {
                return@defer Flux.error<String>(IllegalStateException("File is not opened!"))
            }
        }.doOnNext { n: String -> println("Next line: $n") }
    }

    fun closeFile(): Mono<Void> {
        return Mono.fromRunnable<Void> {
            fileClosed.set(true)
            println("File closed!")
        }.delaySubscription(Duration.ofMillis(500))
    }

    fun tasksToExecute(): Flux<Mono<String>> {
        return Flux.range(1, 3)
            .map { i: Int ->
                Mono.fromSupplier {
                    println("Executing task: #$i")
                    "task#$i"
                }
                    .delaySubscription(Duration.ofMillis(250))
                    .subscribeOn(Schedulers.parallel())
            }
    }

    fun commitTask(taskId: String): Mono<Void> {
        committedTasksCounter.incrementAndGet()
        return Mono.fromRunnable { println("Task committed:$taskId") }
    }

    fun microsoftTitles(): Flux<String> {
        return Flux.just("windows12", "bing2", "office366")
            .doOnNext { title: String -> println("Realising: $title") }
            .delayElements(Duration.ofMillis(150))
            .delaySubscription(Duration.ofMillis(250))
    }

    fun blizzardTitles(): Flux<String> {
        return Flux.just("wow2", "overwatch3", "warcraft4")
            .doOnNext { title: String -> println("Realising: $title") }
            .delayElements(Duration.ofMillis(150))
            .delaySubscription(Duration.ofMillis(350))
    }

    fun carChassisProducer(): Flux<Chassis> {
        return Flux.range(1, 3)
            .delayElements(Duration.ofMillis(350))
            .map { i: Int? -> Chassis(UUID.randomUUID()) }
            .doOnNext { c: Chassis -> println("Chassis produced! #" + c.vin) }
    }

    fun carEngineProducer(): Flux<Engine> {
        return Flux.range(1, 3)
            .delayElements(Duration.ofMillis(550))
            .map { i: Int? -> Engine(UUID.randomUUID()) }
            .doOnNext { e: Engine -> println("Engine produced! #" + e.vin) }
    }

    fun sourceA(): Mono<String> {
        return Mono.just("A")
    }

    fun sourceB(): Mono<String> {
        return Mono.just("B")
    }

    class Message(metaData: String, payload: String) {
        @JvmField
        var metaData = ""
        var payload = ""

        init {
            this.metaData = metaData
            this.payload = payload
        }
    }

    class Chassis(var vin: UUID) {
        var seqNum = 0

        init {
            seqNum = ++i
        }

        companion object {
            private var i = 0
        }
    }

    class Engine(var vin: UUID) {
        var seqNum = 0

        init {
            seqNum = ++i
        }

        companion object {
            private var i = 0
        }
    }

    class Car(@JvmField var chassis: Chassis, @JvmField var engine: Engine)
    object StreamingConnection {
        @JvmField
        var isOpen = AtomicBoolean()
        @JvmField
        var cleanedUp = AtomicBoolean()
        @JvmStatic
        fun startStreaming(): Mono<Flux<String>> {
            return Mono.just(Flux.range(1, 20).map { i: Int -> "Message #$i" }
                .delayElements(Duration.ofMillis(250))
                .doOnNext { s: String -> println("Sending message: $s") }
                .doFirst {
                    println("Streaming started!")
                    isOpen.set(true)
                })
        }

        @JvmStatic
        fun closeConnection(): Mono<Void> {
            return Mono.empty<Any>().doFirst {
                println("Streaming stopped! Cleaning up...")
                cleanedUp.set(true)
            }.then()
        }
    }
}
