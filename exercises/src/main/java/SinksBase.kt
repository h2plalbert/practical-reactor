import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * @author Stefan Dragisic
 */
open class SinksBase {
    fun submitOperation(operation: Runnable?) {
        Executors.newScheduledThreadPool(1).schedule(operation, 5, TimeUnit.SECONDS)
    }

    //don't change me
    fun doSomeWork() {
        println("Doing some work")
    }

    //don't change me
    fun get_measures_readings(): List<Int> {
        println("Reading measurements...")
        println("Got: 0x0800")
        println("Got: 0x0B64")
        println("Got: 0x0504")
        return mutableListOf(0x0800, 0x0B64, 0x0504)
    }
}
