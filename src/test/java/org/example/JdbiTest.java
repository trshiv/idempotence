package org.example;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Map;

public class JdbiTest {
    public static void main(String[] args) {
        Jdbi jdbi = Jdbi.create("jdbc:hsqldb:mem:testDB");
        List<Map<String, Object>> entries = jdbi.withHandle(handle -> {
           handle.execute("CREATE TABLE idempotence (id VARCHAR(100) PRIMARY KEY, type VARCHAR(10), value VARCHAR(1024))");
           handle.execute("INSERT INTO idempotence(id, type, value) VALUES(?, ?, ?)", "1", "request", "request-body");
           return handle.createQuery("SELECT * from idempotence").mapToMap().list();
       });
        entries.stream().forEach(System.out::println);
    }
}
