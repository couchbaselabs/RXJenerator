import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import rx.Observable;
import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CannotRetryException;
import com.couchbase.client.java.util.retry.RetryBuilder;

//Running with defaults = 14k ops/s
//java -cp java_loadgen.jar RXJavaLoadGen.RXJenerator -t 5000 -n localhost -l OFF -c 2000
//java -cp java_loadgen.jar RXJavaLoadGen.RXJenerator -t 5000 -n localhost -l SEVERE -c 2000 -easydebug
//java -cp java_loadgen.jar RXJavaLoadGen.RXJenerator -t 5000 -n localhost -l SEVERE -c 2000 -forcebackpressure
//can not run 3 instances without lots of backpressure exceptions! -> I may need to maximize to XXk per instance :-(, or maybe choose a longer sleep_time

public class RXJenerator {

    private static final Logger LOGGER = Logger.getLogger(RXJenerator.class.getName());
    private static Level logLevel = Level.OFF;
    private static List<String> nodes = new ArrayList<String>(Arrays.asList(new String[]{"localhost"}));
    private static CouchbaseEnvironment env;

    private static enum RetryStrategy {
        FAST_FAIL, BEST_EFFORT
    }

    public static RetryStrategy retryStrategy = RetryStrategy.BEST_EFFORT;
    private static Cluster cluster;
    private static Bucket bucket;
    private static String bucketName = "default";
    private static String bucketPassword = "";

    private static boolean run_bulk_write_test = true;
    private static boolean run_bulk_read_test = true;
    private static long load_divider = 1; //set this to 2 if(run_bulk_write_test && run_bulk_read_test) because we have double the load because of both reads/writes
    private static float load_compensation = 1.8f; //80% additional load to account for overhead of logging to the console and inconsistency of thread.sleep

    private static final int ONE_SECOND = 1000;
    private static float throughput_per_second = 10_000;
    //private static long sleep_time = 10; //nice flat curve
    //private static long sleep_time = 20; //still nice flat curve
    //private static long sleep_time = 50; //still nice flat curve
    //private static long sleep_time = 100; //still nice flat curve
    private static long sleep_time = 500; //still nice flat curve
    private static int keys_per_cycle = (int) (throughput_per_second / ONE_SECOND * sleep_time / load_divider * load_compensation); // (10 keys per 1 ms) * 100ms = 1000 keys per cycle
    private static AtomicInteger number_of_aysnc_cycles_to_run = new AtomicInteger(-1);
    private static int number_of_sync_loops_to_run = -1;
    private static boolean infinite_cycles = true;
    private static boolean print_stats_for_infinate_stream = false;
    private static AtomicInteger complete_cycle_count_for_printing_stats = new AtomicInteger(0); //counter to determine when to print statistics
    private static AtomicInteger print_counter_rate = new AtomicInteger(10); //if max_cycle is infinite (-1) then run print of stats every ~10k times ... print if complete_cycle_count > print_counter_rate

    private static boolean force_back_pressure = false;
    private static boolean force_easy_debug = false;

    private static JsonObject user;
    private static ArrayList<String> doc_ids = new ArrayList<String>();
    private static List<JsonDocument> docs = new ArrayList<>();

    private static AtomicInteger reads = new AtomicInteger(0);
    private static AtomicInteger read_from_replicas = new AtomicInteger(0);
    private static AtomicInteger read_failures = new AtomicInteger(0);
    private static AtomicInteger writes = new AtomicInteger(0);
    private static AtomicInteger write_failures = new AtomicInteger(0);
    private static AtomicInteger timeout_exceptions = new AtomicInteger(0);
    private static AtomicInteger backpressue_exceptions = new AtomicInteger(0);
    private static AtomicInteger cannotretry_exceptions = new AtomicInteger(0);

    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        processArgumentList(args);

        RXJenerator app = new RXJenerator();

        try {
            System.out.println("RXJenerator starting in: ");
            for (int i = 3; i > 0; i--) {
                System.out.println(i);
                Thread.sleep(500);
            }
            System.out.println("GO!");
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        if (run_bulk_write_test) {
            app.bulkWrite();
        }

        if (run_bulk_read_test) {
            app.bulkGet();
        }

        try {
            latch.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } //latch is released either in BulkRead or BulkWrite Subscriber methods. This keeps the main thread alive

        print_counters(0); //Last print of counters
        System.out.println("End of main");

    }

    private static void processArgumentList(String[] args) {

        ArrayList<String> argList = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            argList.add(args[i]);
        }

        int indexOfForceBackPressure = argList.indexOf("-forcebackpressure");
        if (indexOfForceBackPressure != -1) {
            force_back_pressure = true;
        }

        int intForce_easy_debug = argList.indexOf("-easydebug");
        if (intForce_easy_debug != -1) {
            force_easy_debug = true;
        }

        int indexOfLogLevel = argList.indexOf("-l");
        if (indexOfLogLevel != -1) {
            logLevel = Level.parse(argList.get(indexOfLogLevel + 1));
            if (!logLevel.equals(Level.OFF)) {
                System.out.println("Log Level = " + logLevel.getName());
                System.out.println("Note - Logging to the console may impacts applicaton performance and throughput");
            }
        }

        int indexOfnode = argList.indexOf("-n");
        if (indexOfnode != -1) {
            nodes.clear();
            nodes.add(argList.get(indexOfnode + 1));
        }

        int indexOfBucket = argList.indexOf("-b");
        if (indexOfBucket != -1) {
            bucketName = argList.get(indexOfBucket + 1);
        }

        int indexOfBucketPassword = argList.indexOf("-p");
        if (indexOfBucketPassword != -1) {
            bucketPassword = argList.get(indexOfBucketPassword + 1);
        }

        int indexOfThroughputPerSecond = argList.indexOf("-t");
        if (indexOfThroughputPerSecond != -1) {
            throughput_per_second = (int) Long.parseLong(argList.get(indexOfThroughputPerSecond + 1));
        }

        int indexOfMaxCycles = argList.indexOf("-c");
        if (indexOfMaxCycles != -1) {
            number_of_aysnc_cycles_to_run.set(Integer.parseInt(argList.get(indexOfMaxCycles + 1)));
            number_of_sync_loops_to_run = number_of_aysnc_cycles_to_run.get();
            infinite_cycles = false;
        }
    }

    private static void print_counters(int cycle) {
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        if (cycle >= 0) //if infinite - then cycle will be <= -1
            System.out.println("number_of_aysnc_cycles_to_run = " + cycle);
        System.out.println("reads = " + reads.get());
        System.out.println("read_from_replicas = " + read_from_replicas.get());
        System.out.println("read_failures = " + read_failures.get());
        System.out.println("writes = " + writes.get());
        System.out.println("write_failures = " + write_failures.get());
        System.out.println("timeout_exceptions = " + timeout_exceptions.get());
        System.out.println("backpressue_exceptions = " + backpressue_exceptions.get());
        System.out.println("cannotretry_exceptions = " + cannotretry_exceptions.get());
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
    }

    public RXJenerator() {

        LOGGER.setLevel(logLevel); //set logging level for my code
        Logger.getLogger("com.couchbase.client").setLevel(Level.SEVERE); //set logging level for Couchbase client to SEVERE to reduce noise (the default is INFO)

        //Work out the number of keys to bulk process per sleep_time interval
        if (run_bulk_write_test && run_bulk_read_test) {
            load_divider = 2; //because we have double the load
            if (number_of_aysnc_cycles_to_run.get() != -1) {
                number_of_aysnc_cycles_to_run.set(number_of_aysnc_cycles_to_run.get() * 2); //because we need to decrement max_cycles for both reads and writes
                number_of_sync_loops_to_run = number_of_aysnc_cycles_to_run.get();
            }
        }
        if (LOGGER.getLevel() == Level.OFF || LOGGER.getLevel() == Level.SEVERE) {
            load_compensation = 1.4f;// if logging is off or at severe - then we don't as much additional load to account for overhead
        }
        keys_per_cycle = (int) Math.max(throughput_per_second / ONE_SECOND * sleep_time / load_divider * load_compensation, 1);

        if (force_back_pressure) {
            keys_per_cycle = 10000;
            sleep_time = 1;
            number_of_aysnc_cycles_to_run.set(10);//set the number of cycles to something high to ensure we see BackPressureException exceptions
            number_of_sync_loops_to_run = 10;//set the number of cycles to something high to ensure we see BackPressureException exceptions
            LOGGER.setLevel(Level.SEVERE); //in order to see severe messages
        }

        if (force_easy_debug) {
            LOGGER.setLevel(Level.INFO); //override to see my INFO and SEVER messages
            Logger.getLogger("com.couchbase.client").setLevel(Level.OFF); //Removes noise
            keys_per_cycle = 6;    //Limit to less output
            sleep_time = 1000;        //make the output slower too
            number_of_aysnc_cycles_to_run.set(1);//set the number of cycles to something high to ensure we see BackPressureException exceptions
            number_of_sync_loops_to_run = 1;//set the number of cycles to something high to ensure we see BackPressureException exceptions
        }

        //If the number of runs is infinite or over XXX (TODO) -> then we need to set print_stats_for_infinate_stream to true, otherwise we only print counter stats once
        if (number_of_aysnc_cycles_to_run.get() == -1) { //NEED TO ADD A CHECK FOR OVER XXX
            print_stats_for_infinate_stream = true;
        }

        user = JsonObject.empty().put("firstname", "Terry").put("lastname", "Dhariwal").put("job", "Couchbase SE").put("age", 35);
        for (int i = 0; i < keys_per_cycle; i++) {
            doc_ids.add(i + "");
            docs.add(JsonDocument.create(i + "", user));
        }

        if (retryStrategy == RetryStrategy.BEST_EFFORT)
            env = DefaultCouchbaseEnvironment.builder().
                    retryStrategy(BestEffortRetryStrategy.INSTANCE)
                    .ioPoolSize(4) //4 cores left after VMs on my mac
                    .computationPoolSize(4) //4 cores left after VMs on my mac
                    .requestBufferSize(131072) //max size I think
                    .responseBufferSize(131072) //max size I think
                    .build();
        else if (retryStrategy == RetryStrategy.FAST_FAIL)
            env = DefaultCouchbaseEnvironment.builder().
                    retryStrategy(FailFastRetryStrategy.INSTANCE)
                    .ioPoolSize(4) //4 cores left after VMs on my mac
                    .computationPoolSize(4) //4 cores left after VMs on my mac
                    .requestBufferSize(131072) //max size I think
                    .responseBufferSize(131072) //max size I think
                    .build();
        cluster = CouchbaseCluster.create(env, nodes);
        bucket = cluster.openBucket(bucketName, bucketPassword);

        printLoadGenSettings();
    }

    private void printLoadGenSettings() {
        System.out.println("retryStrategy = " + retryStrategy.name());
        System.out.println("keys_per_cycle = " + keys_per_cycle);
        System.out.println("sleep_time = " + sleep_time);
        System.out.println("number_of_aysnc_cycles_to_run = " + number_of_aysnc_cycles_to_run.get());
        System.out.println("number_of_sync_loops_to_run = " + number_of_sync_loops_to_run);
        System.out.println("complete_cycle_count_for_printing_stats = " + complete_cycle_count_for_printing_stats.get()); //counter to determine when to print statistics
        System.out.println("print_counter_rate = " + print_counter_rate.get());
        System.out.println("writes = " + writes.get());
        System.out.println("print_stats_for_infinate_stream = " + print_stats_for_infinate_stream);
    }

    public void bulkWrite() {
        Observable.interval(sleep_time, TimeUnit.MILLISECONDS) //use the default schedular - not newThread or io since these are unbound and can lead to oom
                .map(tick -> {
                    return Observable.from(docs)
                            .doOnNext(emittedJsonDoc -> {
                                LOGGER.info(currenThreadName() + "Bulk write: " + "Upserting doc.id(): " + emittedJsonDoc.id());
                            })
                            .flatMap(doc -> {
                                final long s = System.nanoTime();
                                return Observable.defer(() -> bucket.async().upsert(doc))    //.upsert(doc, PersistTo.MASTER, ReplicateTo.ONE) //Waiting for bug fix
                                        .timeout(2, TimeUnit.SECONDS)        //timeout for write
                                        .doOnError(error -> {                //I need a doOnError here to capture BackPressureExceptions here because the downstream doOnError will not due to the retry
                                            if (error instanceof BackpressureException) {
                                                increment_exception_counters(error); //Note - TimeoutExceptions and CannotRetryException exceptions will be caught downstream (not here to avoid double counting)
                                                LOGGER.severe(currenThreadName() + "Bulk write " + "{Exception: {" + error.getClass() + "}, message: {" + error.getMessage() + "}, document: {" + doc.id() + "}, cause: {" + error.getCause() + "}");
                                            }
                                        })
                                        .retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                                                .delay(Delay.exponential(TimeUnit.MILLISECONDS, 100))
                                                .max(10)
                                                .build())
                                        .retryWhen(RetryBuilder.anyOf(TimeoutException.class)
                                                .once()
                                                .delay(Delay.fixed(100, TimeUnit.MILLISECONDS))
                                                .build())
                                        .throttleWithTimeout(3, TimeUnit.SECONDS) //Make the timeout longer in case we have network issues
                                        .doOnError(error -> { //Need the doOnError here to log TimeoutException and CannotRetryException exceptions
                                            if (!(error instanceof BackpressureException)) {
                                                increment_exception_counters(error); //for incr'ing TimeoutException and CannotRetryException exceptions (will not get BackPressureExceptions here because of upstream retry
                                                LOGGER.severe(currenThreadName() + "Bulk write " + "{Exception: {" + error.getClass() + "}, message: {" + error.getMessage() + "}, document: {" + doc.id() + "}, cause: {" + error.getCause() + "}");
                                            }
                                        })
                                        .onErrorResumeNext(error -> {
                                            write_failures.incrementAndGet();
                                            return Observable.empty();  //must return empty so that doOnNext calls only happen for successful writes
                                        })
                                        .doOnNext(document -> {
                                            writes.getAndIncrement();
                                            LOGGER.info(currenThreadName() + "Bulk write: " + "Upserted document: " + document.id() + " in " + nano_to_milli(s) + " milliseconds");
                                        });
                            })
                            .subscribe(
                                    returnedDocument -> {
                                        LOGGER.info(currenThreadName() + "Bulk write: " + "SUCCESS: Returned document: " + returnedDocument.id() + ", JsonDocument = " + returnedDocument.toString());
                                    },
                                    error -> {
                                        //This should not be called (in theory)
                                        LOGGER.severe(currenThreadName() + "Bulk write: " + "something bad occurred with " + error.toString());
                                        //If we get here something really bad happened and we should kill the mainThread
                                        number_of_aysnc_cycles_to_run.set(0);
                                        number_of_sync_loops_to_run = 0;
                                        latch.countDown();
                                    },
                                    () -> {
                                        int num_cycle = number_of_aysnc_cycles_to_run.decrementAndGet();
                                        complete_cycle_count_for_printing_stats.getAndIncrement();
                                        if (complete_cycle_count_for_printing_stats.get() > print_counter_rate.get()) {
                                            complete_cycle_count_for_printing_stats.set(0); //reset for next print_counter_rate
                                            print_counters(num_cycle);
                                        }
                                        //if(number_of_aysnc_cycles_to_run.get() == 0) {
                                        if (!infinite_cycles && number_of_aysnc_cycles_to_run.get() < 0) {
                                            //RESET LATCH
                                            System.out.println("KILLING MAIN THREAD ...");
                                            latch.countDown();
                                        }
                                    });
                })
                .subscribe();
        //returns immediately
    }

    public void bulkGet() {
        Observable.interval(sleep_time, TimeUnit.MILLISECONDS)
                .map(tick -> {
                    return Observable.from(doc_ids)
                            .flatMap(id -> getWithFallback(id)) //returns a new observable for each emission - so that exceptions can be caught per write - allows for continuation of upstream observable
                            .timeout(1, TimeUnit.MINUTES) //this is a global timeout for the whole list of id's to be retrieved
                            .subscribe(
                                    returnedDocument -> {
                                        LOGGER.info(currenThreadName() + "Bulk read: " + "SUCCESS: Returned document: doc.id() = " + returnedDocument.id() + ", JsonDocument = " + returnedDocument.toString());
                                    },
                                    error -> {
                                        //This should not be called (in theory)
                                        LOGGER.severe(currenThreadName() + "Bulk read: " + "something bad occurred with " + error.toString());
                                        //If we get here something really bad happened and we should kill the mainThread
                                        number_of_aysnc_cycles_to_run.set(0);
                                        number_of_sync_loops_to_run = 0;
                                        latch.countDown();
                                    },
                                    () -> {
                                        int num_cycle = number_of_aysnc_cycles_to_run.decrementAndGet();
                                        complete_cycle_count_for_printing_stats.getAndIncrement();
                                        if (complete_cycle_count_for_printing_stats.get() > print_counter_rate.get()) {
                                            complete_cycle_count_for_printing_stats.set(0); //reset for next print_counter_rate
                                            print_counters(num_cycle);
                                        }
                                        //if(number_of_aysnc_cycles_to_run.get() == 0) {
                                        if (!infinite_cycles && number_of_aysnc_cycles_to_run.get() < 0) {
                                            //RESET LATCH
                                            System.out.println("KILLING MAIN THREAD ...");
                                            latch.countDown();
                                        }
                                    }
                            );
                })
                .subscribe();
        // this returns immediately
    }

    public Observable<JsonDocument> getWithFallback(String id) {
        final long s = System.nanoTime();
        return Observable.defer(() -> bucket.async().get(id))
                .timeout(2, TimeUnit.SECONDS)        //timeout for get(id)
                .doOnError(error -> {                //I need a doOnError here to capture BackPressureExceptions here because the downstream doOnError will not due to the retry
                    if (error instanceof BackpressureException) {
                        increment_exception_counters(error); //Note - TimeoutExceptions and CannotRetryException exceptions will be caught downstream (not here to avoid double counting)
                        LOGGER.severe(currenThreadName() + "Bulk read " + "{Exception: {" + error.getClass() + "}, message: {" + error.getMessage() + "}, document: {" + id + "}, cause: {" + error.getCause() + "}");
                    }
                })
                .retryWhen(RetryBuilder.anyOf(BackpressureException.class).delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(10).build())
                .doOnError(error -> { //Need the doOnError here to log TimeoutException and CannotRetryException exceptions
                    if (!(error instanceof BackpressureException)) {
                        increment_exception_counters(error); //for incr'ing TimeoutException and CannotRetryException exceptions (will not get BackPressureExceptions here because of upstream retry
                        LOGGER.severe(currenThreadName() + "Bulk read " + "{Exception: {" + error.getClass() + "}, message: {" + error.getMessage() + "}, document: {" + id + "}, cause: {" + error.getCause() + "}");
                    }
                })
                .onErrorResumeNext((error) -> {
                    return Observable.defer(() -> bucket.async().getFromReplica(id, ReplicaMode.FIRST)).first() // This works with BestEffortRetryStrategy AND FailFastRetryStrategy (don't need .first()
                            .timeout(2, TimeUnit.SECONDS) //This applies to only the getFromReplica(id ...)
                            .doOnError(error2 -> { //I need a doOnError here to capture BackPressureExceptions here because the downstream doOnError will not due to the retry
                                if (error2 instanceof BackpressureException) {
                                    increment_exception_counters(error2); //Note - TimeoutExceptions and CannotRetryException exceptions will be caught downstream (not here to avoid double counting)
                                    LOGGER.severe(currenThreadName() + "Bulk read " + "{Exception in readFromReplica: {" + error2.getClass() + "}, message: {" + error2.getMessage() + "}, document: {" + id + "}, cause: {" + error2.getCause() + "}");
                                }
                            })
                            .retryWhen(RetryBuilder.anyOf(BackpressureException.class).delay(Delay.exponential(TimeUnit.MILLISECONDS, 100)).max(10).build())
                            .doOnError(error3 -> { //Need the doOnError here to log TimeoutException and CannotRetryException exceptions
                                if (!(error3 instanceof BackpressureException)) {
                                    increment_exception_counters(error3); //for incr'ing TimeoutException and CannotRetryException exceptions (will not get BackPressureExceptions here because of upstream retry
                                    LOGGER.severe(currenThreadName() + "Bulk read " + "{Exception in readFromReplica: {" + error3.getClass() + "}, message: {" + error3.getMessage() + "}, document: {" + id + "}, cause: {" + error3.getCause() + "}");
                                }
                            })
                            .onErrorResumeNext((error4) -> { //Runs to recover from ReadFromReplica failures
                                read_failures.incrementAndGet();
                                return Observable.empty();
                            })
                            .doOnNext(document -> {
                                read_from_replicas.incrementAndGet();
                                LOGGER.info(currenThreadName() + "Bulk read: " + "Read document from replica: " + document.id());
                            });
                })
                .doOnNext(document -> { //only called for successful read (whether from active or replica)
                    reads.getAndIncrement();
                    LOGGER.info(currenThreadName() + "Bulk read: " + "Read document: " + document.id() + " in " + nano_to_milli(s) + " milliseconds");
                });
    }

    private String currenThreadName() {
        String threadName = Thread.currentThread().getName();
        while (threadName.length() < 35) { //while(threadName.length() != 20) {//bug here
            threadName += " ";
        }
        return threadName + " ";
    }

    private long nano_to_milli(final long s) {
        return (System.nanoTime() - s) / 1000000;
    }

    private void increment_exception_counters(Throwable error) {
        if (error instanceof TimeoutException) {
            timeout_exceptions.getAndIncrement();
        } else if (error instanceof BackpressureException) {
            backpressue_exceptions.getAndIncrement();
        } else if (error instanceof CannotRetryException) {
            cannotretry_exceptions.getAndIncrement();
        }
    }

}