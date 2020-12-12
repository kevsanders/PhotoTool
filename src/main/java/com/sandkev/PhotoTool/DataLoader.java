package com.sandkev.PhotoTool;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DataLoader {

    public static String load(String csvFile) throws SQLException {
        //String csvFile="src\\main\\resources\\small-sample.csv";
        String dbUrl = "jdbc:h2:mem:test";
        //dbUrl = "jdbc:h2:file:D:/sources/_github/PhotoTool/build/testDb";
        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setJdbcUrl(dbUrl);
        ds.setUsername("sa");
        ds.setPassword("");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        int tableExists = jdbcTemplate.queryForObject("SELECT COUNT(1) AS count FROM information_schema.tables WHERE table_name = 'file_to_hex'", Integer.class);
        if (tableExists == 0) {
            jdbcTemplate.execute("create table file_to_hex as select * from CSVREAD('" + csvFile + "')");
            jdbcTemplate.execute("create index idx_fth_hex on file_to_hex(hex)");
        }

        StringWriter sw = new StringWriter();
        System.out.println(jdbcTemplate.queryForObject("select count(*) from file_to_hex", Integer.class));
        System.out.println(jdbcTemplate.queryForObject("select count(distinct hex) from file_to_hex", Integer.class));
        String sql1 =
                "select file, hex from file_to_hex fth" +
                        " ,(select hex as dup from file_to_hex group by hex having count(1)>1) as dups" +
                        " where fth.hex=dups.dup" +
                        " order by hex";
//        String sql2 = "select hex as dup from file_to_hex group by hex having count(1)>1" +
//                " order by hex";
//        List<Map<String, Object>> duplicates = getMaps(jdbcTemplate, sql1);
//        for (Map<String, Object> duplicate : duplicates) {
//            //System.out.println(duplicate.get("HEX") + " " + duplicate.get("FILE"));
//            System.out.println(duplicate);
//        }

        jdbcTemplate.execute("call CSVWRITE ( 'D:/sources/_github/PhotoTool/build/dups.txt', '" + sql1 + "' )");
        System.out.println(
                getMaps(jdbcTemplate,
                        "select rank() over(partition by fth.hex order by fth.hex, fth.file) as rank, " +
                                "fth.*" +
                                " from file_to_hex fth where hex like '%_0004a40f073f0cbd8278e782ab319d37'"
                )
        );

        return sw.toString();


    }

    private static List<Map<String, Object>> getMaps(JdbcTemplate jdbcTemplate, String sql) {
        return jdbcTemplate.queryForList(
                sql
        );
    }

    public static String loadX(String csvFile) throws SQLException {
        //String csvFile="src\\main\\resources\\small-sample.csv";
        String jdbcUrl = "jdbc:h2:mem:test";
        jdbcUrl = "jdbc:h2:file:D:/sources/_github/PhotoTool/build/testDb";

        HikariDataSource ds = new HikariDataSource();
        //BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setJdbcUrl(jdbcUrl);
//        ds.setUrl(jdbcUrl);
        ds.setUsername("");
        ds.setPassword("");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        jdbcTemplate.execute("create table file_to_hex as select * from CSVREAD('" + csvFile + "')");

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
/*
            try(Statement statement = con.createStatement()) {
                new Csv().write(pw, statement.executeQuery("SELECT Company" +
                        ",count(case when ActionsType='Downloaded' then 1 else null end) Downloaded" +
                        ",count(case when ActionsType='Watched' then 1 else null end) Watched" +
                        ",count(case when ActionsType='Subscribed' then 1 else null end) Subscribed" +
                        " FROM sample GROUP BY Company"));
            }
*/

        System.out.println(jdbcTemplate.queryForObject("select count(*) from file_to_hex", Integer.class));
        System.out.println(jdbcTemplate.queryForObject("select count(distinct hex) from file_to_hex", Integer.class));

        //jdbcTemplate.execute("ALTER USER admin SET PASSWORD 'password'");

        return sw.toString();
    }

}
