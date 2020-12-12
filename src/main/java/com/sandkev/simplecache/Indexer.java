package com.sandkev.simplecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kevin on 31/03/2019.
 */
public class Indexer implements Closeable {

    private static final Logger logger = LogManager.getLogger();
    public static final int FLUSH_SIZE = 250;

    enum ContextKeys{PARENT_ID, DATA_TYPE}

    private final static ConcurrentHashMap<File, IndexWriter> indexWriters = new ConcurrentHashMap<>();
    private final AtomicInteger uncommittedWrites = new AtomicInteger();
    private final static ConcurrentHashMap<File, SearcherManager> searchManagers = new ConcurrentHashMap<>();
    private final Thread searchManagerUpdater;
    private final File rootDir;

    public Indexer(String storagePath) {

        rootDir = new File(storagePath);

        this.searchManagerUpdater = new Thread(new Runnable() {
            public void run() {
                while (true){
                    try {
                        for (SearcherManager searcherManager : searchManagers.values()) {
                            if(!searcherManager.isSearcherCurrent()){
                                searcherManager.maybeRefresh();
                            }
                        }
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (IOException e) {
                        logger.error("Failed to update search manager. ", e);
                    }
                }
            }
        });
        searchManagerUpdater.setName("SearchManagerUpdater");
        searchManagerUpdater.setDaemon(true);

    }

    public Collection<File> findKeys(String searchText, int offset, int limit) throws IOException {

        String index = "photos";
        List<File> keys = new ArrayList<>();
        //TODO: use cached map of searchManagers so we don't have to open a new reader/searcher each time
        SearcherManager searcherManager = null;
        IndexSearcher searcher = null;
        try{
            searcherManager = getSearcherManager(index);
            Analyzer analyzer = getAnalyzer();

            searcher = searcherManager.acquire();

            //QueryParser parser = new QueryParser("file", analyzer);
           // Query query = parser.parse(searchText);
            Query query = new WildcardQuery(new Term("file", searchText));
            TopDocs topDocs = searcher.search(query, 1000000);

            ScoreDoc[] hits = topDocs.scoreDocs;

            if(hits.length == 0){
                throw new IndexWriterException("File not found [" + searchText + "] at index[" + index + "]");
            }

            int end = Math.min(hits.length, offset + limit);
            for(int n = 0; n < end; n++){
                if(n < offset) continue;//skip ahead to starting point
                Document doc = searcher.doc(hits[n].doc);
//                PhotoDoc photoDoc = PhotoDoc.builder()
//                        .file(doc.get("file"))
//                        .length(doc.get("length"))
//                        .name(doc.get("name"))
//                        .date(doc.get("date"))
//                        .folder(doc.get("folder"))
//                        .crc32(doc.get("crc32"))
//                        .keepFlag(doc.get("keepFlag"))
//                        .build();
                keys.add(new File(doc.get("file")));
            }


        }catch (Throwable e){
            throw new IndexWriterException("Failed to findTrade[" + searchText + "] at index[" + index + "]", e);
        }finally {
            try {
                if(searcherManager!=null)searcherManager.release(searcher);
            } catch (IOException e) {
                logger.warn("Failed to release searchManager", e);
            }
        }

        return keys;
    }

    public List<PhotoDoc> search(String searchQuery, int page, int hitsPerPage){

        String index = "photos";
        List<PhotoDoc> docs = new ArrayList<>();
        SearcherManager searcherManager = null;
        IndexSearcher searcher = null;
        try{
            searcherManager = getSearcherManager(index);
            Analyzer analyzer = getAnalyzer();

            searcher = searcherManager.acquire();

            int MAX_RESULTS = 9999;
            TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_RESULTS, MAX_RESULTS);  // MAX_RESULTS is just an int limiting the total number of hits
            int startIndex = (page -1) * hitsPerPage;  // our page is 1 based - so we need to convert to zero based
            Query query = new QueryParser( "file", analyzer).parse(searchQuery);
            searcher.search(query, collector);
            TopDocs hits = collector.topDocs(startIndex, hitsPerPage);
            for(int n = 0; n < hits.scoreDocs.length; n++){
                Document doc = searcher.doc(hits.scoreDocs[n].doc);
                PhotoDoc photoDoc = PhotoDoc.builder()
                        .file(new File(doc.get("file")))
                        .length(doc.getField("length").numericValue().longValue())
                        .name(doc.get("name"))
                        //.date(doc.getField("date").numericValue().intValue())
                        .date(doc.get("date"))
                        .folder(doc.get("folder"))
                        .crc32(doc.get("crc32"))
                        .keepFlag(doc.getField("keepFlag").numericValue().intValue())
                        .build();
                docs.add(photoDoc);
            }

        }catch (Throwable e){
            throw new IndexWriterException("Failed to findTrade[" + searchQuery + "] at index[" + index + "]", e);
        }finally {
            try {
                if(searcherManager!=null)searcherManager.release(searcher);
            } catch (IOException e) {
                logger.warn("Failed to release searchManager", e);
            }
        }

        return docs;
    }

    //------ index functions

    public void addIndex(PhotoDoc photo) throws IndexWriterException {

        IndexWriter writer = getIndexWriter("photos");
        if(writer==null){
            logger.error("writer is null!!");
        }

        String key = photo.getFile().getAbsolutePath();
        Document doc = new Document();
        doc.add(new StringField("file", key, Field.Store.YES));
        doc.add(new StringField("length", String.valueOf(photo.getLength()), Field.Store.YES));
        doc.add(new StringField("crc32", photo.crc32, Field.Store.YES));
        doc.add(new StringField("name", photo.name, Field.Store.YES));
        //doc.add(new StringField("folder", photo.folder, Field.Store.YES));
        doc.add(new StringField("keepFlag", String.valueOf(photo.keepFlag), Field.Store.YES));

        try {
            writer.updateDocument(new Term("file", key), doc);

            int count = uncommittedWrites.getAndIncrement();
            if( count > FLUSH_SIZE){
                flush(writer);
            }
        }catch (IOException e){
            IndexWriterException indexWriterException = new IndexWriterException(e);
            indexWriterException.initCause(e);
            throw indexWriterException;
        }

    }

    public void closeSearchManagers() throws IndexWriterException {
        IndexWriterException thrownException = null;
        for (Map.Entry<File, SearcherManager> entry : searchManagers.entrySet()) {
            SearcherManager current = entry.getValue();
            try {
                current.close();
            } catch (IOException e) {
                thrownException = new IndexWriterException(e);
            }
        }
        if(thrownException != null){
            throw thrownException;
        }
    }

    private void closeIndexWriters() throws IndexWriterException {
        IndexWriterException thrownException = null;
        for (Map.Entry<File, IndexWriter> entry : indexWriters.entrySet()) {
            IndexWriter current = entry.getValue();
            try {
                closeIndexWriter(entry, current);
            } catch (IndexWriterException e) {
                thrownException = e;
            }
        }
        if(thrownException != null){
            throw thrownException;
        }
    }

    private void closeIndexWriter(Map.Entry<File, IndexWriter> entry, IndexWriter current) throws IndexWriterException {
        try {
            logger.info("merging index [{}] - start", entry.getKey());
            current.forceMerge(1);
            current.commit();
            logger.info("merging index [{}] - end", entry.getKey());
        }catch (Exception e){
            logger.warn("Failed to merge index", e);
        }finally {
            try{
                current.close();
            }catch (Exception ignore){}
        }
    }

    public void flush(){
        IOException thrownException = null;
        for (Map.Entry<File, IndexWriter> entry : indexWriters.entrySet()) {
            IndexWriter current = entry.getValue();
            try {
                flush(current);
            } catch (IOException e) {
                thrownException = e;
            }
        }
        if(thrownException != null){
            throw new IndexWriterException(thrownException);
        }

    }
    private synchronized void flush(IndexWriter writer) throws IOException {
        writer.commit(); //TODO commit periodically
        uncommittedWrites.getAndSet(0);
    }

    private IndexWriter getIndexWriter(String index) throws IndexWriterException {

        //create file for the index path
        final File indexPath = createPath(rootDir,
                asArray("index", index),
                true);

        //check to see if the appropriate
        try {
            IndexWriter indexWriter = indexWriters.get(indexPath);
            while(indexWriter == null){
                if(indexWriter == null){
                    Analyzer analyzer = getAnalyzer();
                    Directory directory = FSDirectory.open(indexPath.toPath());
                    IndexWriterConfig config = new IndexWriterConfig(analyzer);
                    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                    config.setRAMBufferSizeMB(256.0);
                    try {
                        indexWriter = new IndexWriter(directory, config);
                        indexWriter.getAnalyzer();
                        IndexWriter previousWriter = indexWriters.putIfAbsent(indexPath, indexWriter);
                        if(previousWriter!=null){
                            //another thread beat us to it
                            logger.info("Another thread already created the writer {}", previousWriter);
                        }
                    } catch (IOException e) {
                        logger.info("Another thread already created the writer {}", e.getMessage());
                    }
                    indexWriter = indexWriters.get(indexPath);
                    //TODO: use Atomic update on concurrent item
                }
            }
            return indexWriter;
        }catch (IOException e){
            throw new IndexWriterException(e);
        }
    }

    private SearcherManager getSearcherManager(String index) throws IOException {
        //create file for the index path
        final File indexPath = createPath(rootDir,
                asArray("index", index),
                true);

        //check to see if the appropriate
        SearcherManager searcherManager = searchManagers.get(indexPath);
        while (searcherManager == null){
            try {
                searcherManager = new SearcherManager(FSDirectory.open(indexPath.toPath()), null);
                SearcherManager previousSearcherManager = searchManagers.putIfAbsent(indexPath, searcherManager);
                if(previousSearcherManager!=null){
                    //another thread beat us to it
                    logger.info("Another thread already created the searcherManager {}", previousSearcherManager);
                    searcherManager.close();
                }
            } catch (IOException e) {
                logger.info("Another thread already created the searcherManager {}", e.getMessage());
            }
            searcherManager = searchManagers.get(indexPath);
        }
        if(!searcherManager.isSearcherCurrent()){
            searcherManager.maybeRefresh();
        }
        return searcherManager;
    }

    private Analyzer getAnalyzer() {
        return new WhitespaceAnalyzer();
    }


    private String[] asArray(String... elements) {
        return elements;
    }


    //------- file path functions
    /**
     * build a file path from the rootDir for each element of the tuple
     * the last element of the tuple is the file name
     *
     *
     * @param rootDir
     * @param tuple
     * @param createIfMissing
     * @return
     * @throws IOException
     */
    private File createPath(File rootDir, String[] tuple, boolean createIfMissing) throws IndexWriterException {
        File path = rootDir;
        for (int n = 0; n < tuple.length - 1; n++) {
            path = checkPath(path, tuple[n], createIfMissing);
        }
        return new File(path, cleanFileName(tuple[tuple.length - 1]));
    }

    public static String cleanFileName(String s){
        return s.replaceAll("[^a-zA-Z0-9_+-.]", "_");
    }

    private static File checkPath(File root, String folder, boolean createIfMissing) throws IndexWriterException {
        File path = new File(root, cleanFileName(folder));
        IOException thrown = null;
        for (int retry = 0; retry < 3; retry++) {
            thrown = null;
            try {
                if (!path.exists() && createIfMissing) {
                    boolean created = path.mkdirs();
                    if (!created && !path.exists()) {
                        throw new IOException("Failed to create new folder " + path);
                    }
                }
            } catch (IOException e) {
                logger.warn(e.getMessage() + ". attempt {}", retry);
                //could be NFS so sleep a bit
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupt) {
                }
                thrown = e;
            }
        }
        if (thrown != null) {
            throw new IndexWriterException(thrown);
        }
        return path;
    }


    public void close() throws IndexWriterException {
        try {
            closeIndexWriters();
        } catch (IndexWriterException ignore) {
            logger.warn("Exception while closing IndexWriters: ",ignore);
        }
        try {
            closeSearchManagers();
        } catch (IndexWriterException ignore) {
            logger.warn("Exception whhile closing SearchManagers: ",ignore);
        }
    }

}
