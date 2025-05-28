import java.sql.*;
import java.io.*;
import java.util.*;
import com.opencsv.*;

public class KafkaToSQLite {

    static final String DB_PATH = "game_events.db";
    static final String[] EVENT_TYPES = {
            "SessionStart", "LevelComplete", "InAppPurchase", "SocialInteraction", "SessionEnd"
    };

    public static void main(tring[] args) throws Exception {
        long totalStart = System.currentTimeMillis();

        // 初始化 SQLite
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("PRAGMA journal_mode = WAL;");
        stmt.execute("PRAGMA synchronous = NORMAL;");
        for (String et : EVENT_TYPES) {
            long start = System.currentTimeMillis();

            String tableName = "dwd_events_" + et;
            String csvPath = "./output/" + tableName + "/part-00000*.csv"; // or glob parquet-to-csv result

            // 删除旧表3
            stmt.execute("DROP TABLE IF EXISTS " + tableName);

            // 创建表
            stmt.execute("CREATE TABLE " + tableName + " (" +
                    "EventID TEXT, PlayerID TEXT, EventTimestamp TEXT, " +
                    "EventType TEXT, EventDetails TEXT, DeviceType TEXT, " +
                    "Location TEXT, dt TEXT);");

            // 读取 CSV 并插入
            File folder = new File("./output/" + tableName);
            for (File file : Objects.requireNonNull(folder.listFiles())) {
                if (!file.getName().endsWith(".csv")) continue;

                CSVReader reader = new CSVReader(new FileReader(file));
                List<String[]> rows = reader.readAll();
                if (rows.isEmpty()) continue;

                String[] headers = rows.get(0);
                String placeholders = String.join(",", Collections.nCopies(headers.length, "?"));
                String sql = "INSERT INTO " + tableName + " VALUES (" + placeholders + ")";
                PreparedStatement ps = conn.prepareStatement(sql);

                for (int i = 1; i < rows.size(); i++) {
                    String[] row = rows.get(i);
                    for (int j = 0; j < headers.length; j++) {
                        ps.setString(j + 1, row[j]);
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.close();
                reader.close();
            }

            conn.commit();
            System.out.println("写入 " + tableName + " 耗时 " + (System.currentTimeMillis() - start) + "ms");
        }

        conn.close();
        System.out.println("总耗时: " + (System.currentTimeMillis() - totalStart) + "ms");
    }
}
