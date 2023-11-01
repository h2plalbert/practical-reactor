import CombiningPublishersBase.StreamingConnection.closeConnection
import CombiningPublishersBase.StreamingConnection.startStreaming
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import reactor.blockhound.BlockHound
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function
import java.util.function.Supplier

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
class c6_CombiningPublishers : CombiningPublishersBase() {
    /**
     * Goal of this exercise is to retrieve e-mail of currently logged-in user.
     * `getCurrentUser()` method retrieves currently logged-in user
     * and `getUserEmail()` will return e-mail for given user.
     *
     * No blocking operators, no subscribe operator!
     * You may only use `flatMap()` operator.
     */
    @Test
    fun behold_flatmap() {
        Hooks.enableContextLossTracking() //used for testing - detects if you are cheating!

        //todo: feel free to change code as you need
        val currentUserEmail: Mono<String>? = currentUser.flatMap { getUserEmail(it) }

        //don't change below this line
        StepVerifier.create(currentUserEmail)
            .expectNext("user123@gmail.com")
            .verifyComplete()
    }

    /**
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute them.
     *
     * Answer:
     * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
     */
    @Test
    fun task_executor() {
        //todo: feel free to change code as you need
        val tasks: Flux<Void>? = taskExecutor().flatMap { it }

        //don't change below this line
        StepVerifier.create(tasks)
            .verifyComplete()
        Assertions.assertEquals(taskCounter.get(), 10)
    }

    /**
     * `streamingService()` opens a connection to the data provider.
     * Once connection is established you will be able to collect messages from stream.
     *
     * Establish connection and get all messages from data provider stream!
     */
    @Test
    fun streaming_service() {
        //todo: feel free to change code as you need
        val messageFlux: Flux<Message>? = streamingService().flatMapMany { it.map { m -> m } }


        //don't change below this line
        StepVerifier.create(messageFlux)
            .expectNextCount(10)
            .verifyComplete()
    }

    /**
     * Join results from services `numberService1()` and `numberService2()` end-to-end.
     * First `numberService1` emits elements and then `numberService2`. (no interleaving)
     *
     * Bonus: There are two ways to do this, check out both!
     */
    @Test
    fun i_am_rubber_you_are_glue() {
        //todo: feel free to change code as you need
        val numbers: Flux<Int>? = numberService1().concatWith(numberService2())
//        val numbers: Flux<Int>? = Flux.concat(numberService1(),numberService2())


        //don't change below this line
        StepVerifier.create(numbers)
            .expectNext(1, 2, 3, 4, 5, 6, 7)
            .verifyComplete()
    }

    /**
     * Similar to previous task:
     *
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute each of them.
     *
     * Instead of flatMap() use concatMap() operator.
     *
     * Answer:
     * - What is difference between concatMap() and flatMap()?
     * - What is difference between concatMap() and flatMapSequential()?
     * - Why doesn't Mono have concatMap() operator?
     */
    @Test
    fun task_executor_again() {
        //todo: feel free to change code as you need
        val tasks: Flux<Void>? = taskExecutor().concatMap { it }

        //don't change below this line
        StepVerifier.create(tasks)
            .verifyComplete()
        Assertions.assertEquals(taskCounter.get(), 10)
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    fun need_for_speed() {
        //todo: feel free to change code as you need
        val stonks: Flux<String>? = Flux.firstWithValue(stocksGrpc, stocksRest)

        //don't change below this line
        StepVerifier.create(stonks)
            .expectNextCount(5)
            .verifyComplete()
    }

    /**
     * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
     * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
     * `getStocksRest()`.
     */
    @Test
    fun plan_b() {
        //todo: feel free to change code as you need
        val stonks: Flux<String>? = stocksLocalCache.switchIfEmpty(stocksRest)

        //don't change below this line
        StepVerifier.create(stonks)
            .expectNextCount(6)
            .verifyComplete()
        Assertions.assertTrue(localCacheCalled.get())
    }

    /**
     * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
     * switch to a second mailbox. Otherwise, read all messages from first mailbox.
     */
    @Test
    fun mail_box_switcher() {
        //todo: feel free to change code as you need
        val myMail: Flux<Message>? = mailBoxPrimary().switchOnFirst { t, u ->
            t.get()?.takeIf { it.metaData == "spam" }?.let { return@switchOnFirst mailBoxSecondary() } ?: u
        }

        //don't change below this line
        StepVerifier.create(myMail)
            .expectNextMatches { m: Message -> m.metaData != "spam" }
            .expectNextMatches { m: Message -> m.metaData != "spam" }
            .verifyComplete()
        Assertions.assertEquals(1, consumedSpamCounter.get())
    }

    /**
     * You are implementing instant search for software company.
     * When user types in a text box results should appear in near real-time with each keystroke.
     *
     * Call `autoComplete()` function for each user input
     * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
     */
    @Test
    fun instant_search() {
        //todo: feel free to change code as you need
        val suggestions =
            userSearchInput().sample(Duration.ofMillis(10)).flatMap { autoComplete(it) }//todo: use one operator only

        //don't change below this line
        StepVerifier.create(suggestions)
            .expectNext("reactor project", "reactive project")
            .verifyComplete()
    }

    /**
     * Code should work, but it should also be easy to read and understand.
     * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * If all operations have been executed successfully return boolean value `true`.
     */
    @Test
    fun prettify() {
        //todo: feel free to change code as you need
        //todo: use when,and,then...
//        val successful: Mono<Boolean>? = openFile().then(writeToFile("0x3522285912341")).then(closeFile()).then(Mono.just(true))
//        val successful: Mono<Boolean>? = openFile().and(writeToFile("0x3522285912341")).and(closeFile()).then(Mono.just(true))
        val successful: Mono<Boolean>? =
            Mono.`when`(openFile(), writeToFile("0x3522285912341"), closeFile()).then(Mono.just(true))
        // currently do not care about execution order

        //don't change below this line
        StepVerifier.create(successful)
            .expectNext(true)
            .verifyComplete()
        Assertions.assertTrue(fileOpened.get())
        Assertions.assertTrue(writtenToFile.get())
        Assertions.assertTrue(fileClosed.get())
    }

    /**
     * Before reading from a file we need to open file first.
     */
    @Test
    fun one_to_n() {
        //todo: feel free to change code as you need
        val fileLines: Flux<String>? = openFile().thenMany(readFile())
        StepVerifier.create(fileLines)
            .expectNext("0x1", "0x2", "0x3")
            .verifyComplete()
    }

    /**
     * Execute all tasks sequentially and after each task have been executed, commit task changes. Don't lose id's of
     * committed tasks, they are needed to further processing!
     */
    @Test
    fun acid_durability() {
        //todo: feel free to change code as you need
        val committedTasksIds: Flux<String>? = tasksToExecute().flatMap {
            it.map { id ->
                commitTask(id)
                id
            }
        }


        //don't change below this line
        StepVerifier.create(committedTasksIds)
            .expectNext("task#1", "task#2", "task#3")
            .verifyComplete()
        Assertions.assertEquals(3, committedTasksCounter.get())
    }

    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    fun major_merger() {
        //todo: feel free to change code as you need
        val microsoftBlizzardCorp = Flux.merge(microsoftTitles(), blizzardTitles())

        //don't change below this line
        StepVerifier.create(microsoftBlizzardCorp)
            .expectNext(
                "windows12",
                "wow2",
                "bing2",
                "overwatch3",
                "office366",
                "warcraft4"
            )
            .verifyComplete()
    }

    /**
     * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
     * manufacturer. You need both parts before you can produce a car.
     * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
     * slower part producer.
     * After both parts arrive connect them to a car.
     */
    @Test
    fun car_factory() {
        //todo: feel free to change code as you need
        val producedCars: Flux<Car>? = Flux.zip(
            carChassisProducer(),
            carEngineProducer()
        ).map { Car(it.t1, it.t2) }

        //don't change below this line
        StepVerifier.create<Car>(producedCars)
            .recordWith(Supplier<Collection<Car>> { ConcurrentLinkedDeque() })
            .expectNextCount(3)
            .expectRecordedMatches { cars: Collection<Car> ->
                cars.stream()
                    .allMatch { car: Car -> car.chassis.seqNum == car.engine.seqNum }
            }
            .verifyComplete()
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */
    //only read from sourceRef
    var sourceRef = AtomicReference("X")

    //todo: implement this method based on instructions
    fun chooseSource(): Mono<String> {
        return Mono.defer {
            when {
                sourceRef.get() == "A" -> sourceA()
                sourceRef.get() == "B" -> sourceB()
                else -> Mono.empty()
            }
        }
    }

    @Test
    fun deterministic() {
        //don't change below this line
        val source = chooseSource()
        sourceRef.set("A")
        StepVerifier.create(source)
            .expectNext("A")
            .verifyComplete()
        sourceRef.set("B")
        StepVerifier.create(source)
            .expectNext("B")
            .verifyComplete()
    }

    /**
     * Sometimes you need to clean up after your self.
     * Open a connection to a streaming service and after all elements have been consumed,
     * close connection (invoke closeConnection()), without blocking.
     *
     * This may look easy...
     */
    @Test
    fun cleanup() {
        BlockHound.install() //don't change this line, blocking = cheating!
        // should run test with JDK 17 and add arg -XX:+AllowRedefinitionToAddDeleteMethods
        // to get BlockHound works

        //todo: feel free to change code as you need
        val stream = startStreaming()
            .flatMapMany(Function.identity()).doOnComplete { closeConnection().subscribe() }

        //don't change below this line
        StepVerifier.create(stream)
            .then { Assertions.assertTrue(StreamingConnection.isOpen.get()) }
            .expectNextCount(20)
            .verifyComplete()
        Assertions.assertTrue(StreamingConnection.cleanedUp.get())
    }
}
