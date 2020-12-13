package com.sandkev.PhotoTool;

import com.sandkev.simplecache.Indexer;
import com.sandkev.simplecache.PhotoDoc;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.h2.tools.Csv;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;


public class Tools {

    private static final Logger log = LoggerFactory.getLogger(Tools.class);
    private File report;
    private Indexer indexer;

    @BeforeEach
    void setUp() {
        //report = new File("build/all-duplicates-v1.csv");
        report = new File("build/all-files-v1.csv");
        String storagePath = "src/main/resources/static";
        indexer = new Indexer(storagePath);
    }

    @Test
    void canFilterDuplicatesFromReport() throws IOException, SQLException {

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

    @Test
    void canDeleteFromReport() throws IOException, SQLException {
        deleteFromReport(report);
    }

    private void deleteFromReport(File report) throws IOException, SQLException {
        ResultSet rs = new Csv().read(new FileReader(report), new String[]{"HashKey", "duplicate", "baseName", "fileDate", "keep"});
        rs.next();//skip the header row
        while (rs.next()) {
            File file = new File(rs.getString("duplicate"));
            int keep = rs.getInt("keep");
            if (keep == 0) {
                if(file.getAbsolutePath().contains("Media")){
                    System.out.println("are you sure you want to delete this one: " + file);
                    System.exit(1);
                }
                System.out.println("deleted: " + file);
                file.delete();
            }

        }
    }

    @Test
    void canCreateIndexFromReport() throws IOException, SQLException {
        doIndex(report, indexer);
        indexer.merge("photos");
        indexer.close();
    }
    private void doIndex(File report, Indexer indexer) throws IOException, SQLException {
        int counter = 0;
        ResultSet rs = new Csv().read(new FileReader(report), new String[]{"HashKey", "duplicate", "baseName", "fileDate", "keep"});
        rs.next();//skip the header row
        Set<File> uniq = new HashSet<>();
        while (rs.next()) {
            File file = new File(rs.getString("duplicate"));
            if(uniq.add(file)) {
                String len_hash = rs.getString("HashKey");
                String[] tokens = len_hash.split("_");
                PhotoDoc photo = PhotoDoc.builder()
                        .file(file)
                        .length(Long.valueOf(tokens[0]))
                        .crc32(tokens[1])
                        //.date(Integer.parseInt(rs.getString("fileDate")))
                        .date(rs.getString("fileDate"))
                        .name(rs.getString("baseName"))
                        .keepFlag(rs.getInt("keep"))
                        .build();
                indexer.addIndex(photo);
                if (counter++ % 1000 == 0) {
                    log.info("indexing [{}]: {}", counter, photo);
                }
            }
        }
    }

    @Test
    void canReadFromIndex() {

    }

    @Test
    void canScanForFiles() throws IOException {

        Collection<File> keys = indexer.findKeys("*", 0, 1000000);
        for (File key : keys) {
            //log.info("found: {}", key);
        }
        log.info("total of {} files", keys.size());

        Collection<PhotoDoc> docs = indexer.search("date:*", 1, 100000);
        log.info("total of {} docs", docs.size());


    }

    public Collection<File> find(String[] paths, String[] wildcards, PhotoTask.FilterType filterType){
        WildcardFileFilter filter = new WildcardFileFilter(wildcards, IOCase.INSENSITIVE);
        Collection<File> files = new HashSet<>();
        for (String path : paths) {
            System.out.print("scanning for files in " + path);
            System.out.flush();
            Collection<File> collection = FileUtils.listFiles(
                    new File(path),
                    filterType == PhotoTask.FilterType.INCLUDE?
                            filter : new NotFileFilter(filter),
                    TrueFileFilter.INSTANCE
            );
            System.out.println(". found " + collection.size() + " files");
            files.addAll(
                    collection
            );
        }
        return files;
    }


}
