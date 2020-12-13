package org.example;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

import java.util.concurrent.*;
import java.util.function.BiConsumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class TransactionTest {
    public static void main(String[] args) {
        try {
            Jdbi db = Jdbi.create("jdbc:hsqldb:mem:testDB");
            db.setTransactionHandler(new SerializableTransactionRunner());

            Handle handle = db.open();
            // Set up some values
            BiConsumer<Handle, Integer> insert = (h, i) -> h.execute("INSERT INTO ints(value) VALUES(?)", i);
            handle.execute("CREATE TABLE ints (value INTEGER)");
            insert.accept(handle, 10);
            insert.accept(handle, 20);

            // Run the following twice in parallel, and synchronize
            ExecutorService executor = Executors.newCachedThreadPool();
            CountDownLatch latch = new CountDownLatch(2);

            Callable<Integer> sumAndInsert = () ->
                    db.inTransaction(TransactionIsolationLevel.SERIALIZABLE, h -> {
                        // Both read initial state of table
                        int sum = h.select("SELECT sum(value) FROM ints").mapTo(int.class).findOnly();

                        // First time through, make sure neither transaction writes until both have read
                        latch.countDown();
                        latch.await();

                        // Now do the write.
                        insert.accept(h, sum);
                        return sum;
                    });

            // Both of these would calculate 10 + 20 = 30, but that violates serialization!
            Future<Integer> result1 = executor.submit(sumAndInsert);
            Future<Integer> result2 = executor.submit(sumAndInsert);

            // One of the transactions gets 30, the other will abort and automatically rerun.
            // On the second attempt it will compute 10 + 20 + 30 = 60, seeing the update from its sibling.
            // This assertion fails under any isolation level below SERIALIZABLE!
            System.out.println("Result 1 = " + result1.get());
            System.out.println("Result 2 = " + result2.get());
            assertThat((result1.get() + result2.get()), is(30 + 60));

            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
