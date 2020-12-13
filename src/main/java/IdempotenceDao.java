import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface IdempotenceDao {
    @SqlUpdate("CREATE TABLE idempotence (id text PRIMARY KEY, type text, value text)")
    void createTable();

    @SqlUpdate("INSERT INTO idempotence(id, type, request) VALUES (?, \"request\", ?)")
    void createRequest(String id, String request);

    @SqlUpdate("INSERT INTO idempotence(id, type, request) VALUES (?, \"response\", ?)")
    void createResponse(String id, String response);
}
