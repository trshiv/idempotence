package org.example;

import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import com.nurkiewicz.asyncretry.RetryExecutor;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class IdempotencyTest {

    private static final String pgDbUrl = "jdbc:postgresql://localhost/testDB?user=postgres&password=postgres";
    private static Jdbi db;

    @BeforeClass
    public static void initialize() {
        createDb();
    }

    @Before
    public void before() {
        truncateTables();
    }

    @Test
    // This test should fail
    // With transaction isolation level SERIALIZABLE, some transactions will fail, irrespective of whether
    // idempotency keys are duplicated
    public void testWithSerializable_WithRepeats_NoRetry() {
        assertThat(getTotalRows(), is(0));

        int maxId = 10;
        int numRuns = 50;
        Random random = new Random();
        Supplier<String> s = () -> {
            String id = String.valueOf(random.nextInt(maxId));  // ensure that some ids are repeated
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.SERIALIZABLE);
        };
        run(s, numRuns);

        // At the end of the runs we expect that there are exactly twice the number of rows as the number of keys
        // Duplicate keys should not go through
        assertThat(getTotalRows(), is(maxId * 2));
    }

    @Test
    // This test should fail for the same reasons as the previous tests i.e. due to SERIALIZABLE
    public void testWithSerializable_NoRepeats_NoRetry() throws Exception {
        assertThat(getTotalRows(), is(0));

        int numRuns = 50;
        Random random = new Random();
        Supplier<String> s = () -> {
            String id = String.valueOf(random.nextInt());  // no ids are repeated
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.SERIALIZABLE);
        };
        run(s, numRuns);  // no retries

        // At the end of the runs we expect that the number of rows equals twice the number of runs
        // Since there are no duplicate keys, each run should create 2 rows - one request row, one response row
        assertThat(getTotalRows(), is(numRuns * 2));
    }

    @Test
    // This test should fail for the same reasons as the previous tests i.e. due to SERIALIZABLE
    public void testWithSerializable_NoRepeats_WithRetry() {
        assertThat(getTotalRows(), is(0));

        int numRuns = 50;
        Random random = new Random();
        Callable<String> s = () -> {
            String id = String.valueOf(random.nextInt());  // no ids are repeated
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.SERIALIZABLE);
        };
        runWithRetry(s, numRuns);  // no retries

        // At the end of the runs we expect that the number of rows equals twice the number of runs
        // Since there are no duplicate keys, each run should create 2 rows - one request row, one response row
        assertThat(getTotalRows(), is(numRuns * 2));
    }

    @Test
    // This test should pass
    // With transaction isolation level as SERIALIZABLE, some transactions can fail due to the strict nature
    // of overlap checks. Retrying the failed transactions a few times can result in success
    public void testWithSerializable_WithRepeats_WithRetry() {
        read();
        assertThat(getTotalRows(), is(0));

        int maxId = 10;
        int numRuns = 50;
        Random random = new Random();
        Callable<String> c = () -> {
            String id = String.valueOf(random.nextInt(maxId));
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.SERIALIZABLE);
        };
        runWithRetry(c, numRuns);  // failed transactions are retried a few times
        assertThat(getTotalRows(), is(maxId * 2));
    }

    @Test
    // This test should pass
    // With transaction isolation level as READ_COMMITTED, transactions are not aborted due to non-serializability
    // Due to repeats, the same idempotency key can occur multiple times, but the transaction will fail due to the
    // primary key constraint on (key, type)
    public void testWithReadCommitted_WithRepeats_NoRetry() {
        assertThat(getTotalRows(), is(0));

        int maxId = 10;
        int numRuns = 50;
        Random random = new Random();
        Supplier<String> s = () -> {
            String id = String.valueOf(random.nextInt(maxId));  // no repeats of key
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.READ_COMMITTED);
        };
        run(s, numRuns);
        assertThat(getTotalRows(), is(maxId * 2));
    }

    @Test
    // This test should pass
    // With transaction isolation level as READ_COMMITTED, transactions are not aborted due to non-serializability
    // Due to repeats, the same idempotency key can occur multiple times, but the transaction will fail due to the
    // primary key constraint on (key, type)
    // Retries will not make the failed transactions succeed as they failed not due to serializability constraints
    public void testWithReadCommitted_WithRepeats_WithRetry() {
        assertThat(getTotalRows(), is(0));

        int maxId = 10;
        int numRuns = 50;
        Random random = new Random();
        Callable<String> c = () -> {
            String id = String.valueOf(random.nextInt(maxId));  // ensure that some ids are repeated
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.READ_COMMITTED);
        };
        runWithRetry(c, numRuns);
        assertThat(getTotalRows(), is(maxId * 2));
    }


    @Test
    // This test should pass
    // With transaction isolation level as READ_COMMITTED, transactions are not aborted due to non-serializability
    // As no key is repeated, no transaction will fail as the primary key constraints are not violated
    // No retries are needed as transactions will not fail
    public void testWithReadCommitted_NoRepeats_NoRetry() {
        assertThat(getTotalRows(), is(0));

        int numRuns = 50;
        Random random = new Random();
        Supplier<String> s = () -> {
            String id = String.valueOf(random.nextInt());  // no repeats of key
            String value = "value-" + id;
            return operation(id, value, TransactionIsolationLevel.READ_COMMITTED);
        };
        run(s, numRuns);
        assertThat(getTotalRows(), is(numRuns * 2));  // all transactions should succeed
    }

    private void run(Supplier s, int numRuns) {
        int max_retries = 5;
        ExecutorService executor = Executors.newCachedThreadPool();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < numRuns; i++) {
            futures.add(CompletableFuture
                    .supplyAsync(s, executor)
                    .exceptionally(e -> e.toString()));
        }

        for (CompletableFuture cf : futures) {
            try {
                System.out.println(cf.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
    }

    private void runWithRetry(Callable c, int numRuns) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        RetryExecutor executor = new AsyncRetryExecutor(scheduler)
                .retryOn(ExecutionException.class)
                .withExponentialBackoff(500, 2)
                .withMaxDelay(3000)
                .withUniformJitter()
                .withMaxRetries(20);

        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < numRuns; i++) {
            futures.add(executor.getWithRetry(c));
        }

        for (CompletableFuture cf : futures) {
            try {
                System.out.println(cf.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        scheduler.shutdown();
    }

    private static void createDb() {
        db = Jdbi.create(pgDbUrl);
        db.withHandle(handle -> {
            handle.execute("DROP TABLE IF EXISTS idempotency2");
            handle.execute("CREATE TABLE IF NOT EXISTS idempotency2 (key VARCHAR(100) PRIMARY KEY, request VARCHAR(100), response VARCHAR(100))");
            return null;
        });
    }

    private static void truncateTables() {
        db.withHandle(handle -> handle.execute("TRUNCATE TABLE idempotency2"));
    }

    private int getTotalRows() {
        return db.withHandle(handle -> handle.createQuery("SELECT COUNT(1) FROM idempotency2")
                .mapTo(Integer.TYPE)
                .list().get(0));
    }

    //
    // This method simulates the full idempotent operation
    // id - idempotency key
    // value - the request body
    private String operation(String id, String value, TransactionIsolationLevel txnIsolationlevel) {
        String checkQuery = "SELECT * from idempotency2 WHERE key = ?";
        AtomicReference<String> response = new AtomicReference<>();
        try {
            db.inTransaction(txnIsolationlevel, handle -> {
                //handle.getConnection().setAutoCommit(false);

                System.out.println("Beginning transaction with id = " + id + ", value = " + value);
                List<Map<String, Object>> entries = handle.createQuery(checkQuery)
                        .bind(0, id)
                        .mapToMap()
                        .list();
                if (entries.size() == 1) {
                    System.out.println("Idempotency key " + id + " already exists, avoiding retry and returning response");
                    response.set(handle.createQuery("SELECT response FROM idempotency2 WHERE key = ?")
                            .bind(0, id)
                            .mapTo(String.class)
                            .toString());
                } else {
                    System.out.println("Idempotency key " + id + " does not exist, performing operation");
                    handle.createUpdate("INSERT into idempotency2 (key, request) values(?, ?)")
                            .bind(0, id)
                            .bind(1, "request-" + value)
                            .execute();

                    String result = "response-" + value;
                    System.out.println("Updating result as " + result);
                    handle.createUpdate("UPDATE idempotency2 set response=? WHERE key=?")
                            .bind(0, "response-")
                            .bind(1, id)
                            .execute();

                    response.set(result);
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Ending transaction");
            return response.get();
        }
    }

    private static void read() {
        db.withHandle(handle -> {
            handle.createQuery("SELECT * from idempotency2 ORDER BY key, type")
                    .mapToMap()
                    .list()
                    .stream()
                    .forEach(System.out::println);
            return null;
        });
    }
}
