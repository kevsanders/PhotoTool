package com.sandkev.PhotoTool;

import org.h2.tools.Csv;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

public class Tools {

    @Test
    void canFilterDuplicatesFromReport() throws IOException, SQLException {

        File report = new File("build/all-files-v2.csv");
        File cleaned = new File(report + ".cleaned");
        FileWriter fw = new FileWriter(cleaned);
        fw.write("HashKey,duplicate,baseName,fileDate,keep\n");

        //sometimes the report is run multiple times and files are written more than once
        //so we filter by the first occurence
        int counter = 0;
        ResultSet rs = new Csv().read(new FileReader(report), new String[]{"HashKey", "duplicate", "baseName", "fileDate", "keep"});
        rs.next();//skip the header row
        Set<File> uniq = new HashSet<>();
        while (rs.next()) {
            File file = new File(rs.getString("duplicate"));
            if(uniq.add(file)) {
                String line = format("%s,\"%s\",\"%s\",%s,%s",
                        rs.getString("HashKey"),
                        file.getAbsoluteFile(),
                        rs.getString("baseName"),
                        rs.getString("fileDate")==null?"":rs.getString("fileDate"),
                        rs.getString("keep"));
                fw.write(line + "\n");
            }
        }
        fw.flush();
        fw.close();

    }
}
