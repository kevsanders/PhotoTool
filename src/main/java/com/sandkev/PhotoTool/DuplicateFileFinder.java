package com.sandkev.PhotoTool;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.h2.tools.Csv;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.sandkev.PhotoTool.ExifTool.Feature.STAY_OPEN;
import static java.lang.String.format;

/**
 * finds and hashes files
 * will optionally only hash those of same length as an optimization to help identify duplicates.
 */
@Slf4j
public class DuplicateFileFinder {

    private static final ExifTool.Tag[] TAGS = new ExifTool.Tag[]{ExifTool.Tag.DATE_TIME_ORIGINAL, ExifTool.Tag.CREATE_DATE, ExifTool.Tag.MODIFY_DATE};

    private static AtomicReference<HashProvider> HASH_PROVIDER = new AtomicReference<>();
    private static AtomicReference<String> HASH_ALGO = new AtomicReference<>();
    private static AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();
    private static AtomicReference<Integer> TOTAL = new AtomicReference<>();
    private static AtomicReference<File> OUT = new AtomicReference<>();
    private static AtomicReference<Boolean> DUPLICATES_ONLY = new AtomicReference<>();

    private final PhotoTask task;

    public DuplicateFileFinder(PhotoTask task) {
        HASH_PROVIDER.set(task.getHashProvider());
        HASH_ALGO.set(task.getHashProvider().describe());
        this.task = task;
    }

    /**
     * This will find a list of duplicate files.
     * or will calculate the hash for a set of file
     */
    public void findDuplicateFilesList() throws InterruptedException, IOException {

        Collection<File> files = task.find();

        // First create the map for the file size and file reference in the array list.
        Multimap<Long, File> fileSizeMap = collectBySize(files);

        TOTAL.set(fileSizeMap.asMap().keySet().size());
        CountDownLatch latch = new CountDownLatch(TOTAL.get());
        LATCH.set(latch);

        //reduce processing to only look for potential duplicates
        DUPLICATES_ONLY.set(task.isDuplicatesOnly());

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 32;

        // Construct the Disruptor
        Disruptor<DuplicateCheckEvent> disruptor = new Disruptor<>(DuplicateCheckEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        //prepare the results file
        final File out = new File("build/" + task.getReportFile());
        if (!out.exists()) {
            FileUtils.write(out, "HashKey,duplicate,baseName,fileDate,keep\n", StandardCharsets.UTF_8);
        }
        OUT.set(out);

        //single thread to consolidate the results to a file
        EventHandler collectionHandler = (EventHandler<DuplicateCheckEvent>)
                (event, sequence, endOfBatch) -> reduce(event, sequence, endOfBatch);

        int WORKERS = 3;
        //create worker pool to calculate hash for each set of files with same length
        List<PartitionedHasher> hashCalculators = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) {
            hashCalculators.add(new PartitionedHasher(i, WORKERS));
        }
        PartitionedHasher[] hashers = hashCalculators.toArray(new PartitionedHasher[0]);

        //create worker pool to resolve the best guess file date
        List<PartitionedDateResolver> dateResolverList = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) {
            dateResolverList.add(new PartitionedDateResolver(i, WORKERS));
        }
        PartitionedDateResolver[] dateResolvers = dateResolverList.toArray(new PartitionedDateResolver[0]);

        disruptor.
                handleEventsWith(hashers)
                .then(dateResolvers)
                .then(collectionHandler);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<DuplicateCheckEvent> ringBuffer = disruptor.getRingBuffer();

        for (Map.Entry<Long, Collection<File>> entry : fileSizeMap.asMap().entrySet()) {
            ringBuffer.publishEvent(DuplicateFileFinder::translate, entry);
        }

        try {
            //latch.await(60, TimeUnit.SECONDS);
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("processing took too long");;
        }

    }

    static void translate(DuplicateCheckEvent event, long sequence, Map.Entry<Long, Collection<File>> filesOfSameLength) {
        event.getFilesOfSameLength().addAll(filesOfSameLength.getValue());
        event.setFileLength(filesOfSameLength.getKey());
    }

    static void calculateHash(DuplicateCheckEvent event, long sequence, boolean endOfBatch) {
        log.info("calculating hash {}/{}", sequence + 1, TOTAL.get());
        Long fileLength = event.getFileLength();
        Multimap<HashKey, File> filesByHash = event.filesByHash;
        for (File file : event.getFilesOfSameLength()) {
            try {
                String hashHex = HASH_PROVIDER.get().getHashHex(file);
                HashKey key = new HashKey(fileLength, hashHex, HASH_ALGO.get()); //garbage
                filesByHash.put(key, file);
            } catch (IOException e) {
                log.error("Failed to hash file {}", file, e);
            }
        }
        event.setFilesByHash(filesByHash);
    }

    static void resolveDates(DuplicateCheckEvent event, long sequence, boolean endOfBatch, ExifTool tool) {
        log.info("resolving dates {}/{}", sequence + 1, TOTAL.get());
        //TODO: for those with same hash and name check if dates are same
        //if hash and name and date match then these really are duplicates - pick the best one
        //best is from original collection
        //next is one in correct date folder from google (prefer complte name match rather than derived (eg not *(1) or *-edited)
        //otherwise any will do
        Map<File, String> dateByFile = event.dateByFile;
        Multimap<String, File> filesByName = event.filesByName;
        Multimap<String, File> filesByDate = event.filesByDate;
        Multimap<HashKey, File> filesByHash = event.getFilesByHash();
        Map<HashKey, File> keepByHash = event.keepByHash;
        for (Map.Entry<HashKey, Collection<File>> entry : filesByHash.asMap().entrySet()) {
            HashKey key = entry.getKey();
            Collection<File> duplicates = entry.getValue();

            if (DUPLICATES_ONLY.get() && duplicates.size() == 1) {
                continue; //skip this one as it is not a duplicate
            }

            for (File file : duplicates) {
                //if there is more than 1 file in this group with same hash then do fileName cleaning
                String baseName = duplicates.size() > 1 ?
                        file.getName().replaceAll("\\([0-9]\\)", "") :
                        file.getName();
                filesByName.put(baseName, file);
                Map<ExifTool.Tag, String> imageMeta;
                try {
                    imageMeta = tool.getImageMeta(file, TAGS);
                    String fileDate;
                    if(imageMeta.containsKey(ExifTool.Tag.DATE_TIME_ORIGINAL)){
                        fileDate = imageMeta.get(ExifTool.Tag.DATE_TIME_ORIGINAL).replace(":", "-").substring(0,10);
                    } else if(imageMeta.containsKey(ExifTool.Tag.CREATE_DATE)) {
                        fileDate = imageMeta.get(ExifTool.Tag.CREATE_DATE).replace(":", "-").substring(0,10);
                    } else if(imageMeta.containsKey(ExifTool.Tag.MODIFY_DATE)) {
                        fileDate = imageMeta.get(ExifTool.Tag.MODIFY_DATE).replace(":", "-").substring(0,10);
                    } else if (file.getAbsolutePath().contains("Google")){
                        fileDate = file.getParentFile().getName().substring(0,10); //assumes google stores in date based folder
                    } else {
                        fileDate = "";
                    }
                    filesByDate.put(fileDate, file);
                    dateByFile.put(file, fileDate);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(filesByDate.asMap().keySet().size()==1 && filesByName.asMap().keySet().size()==1){

                //these files are duplicates (same hash, name and date)
                String baseName = filesByName.keys().iterator().next();
                for (Collection<File> files : filesByDate.asMap().values()) {
                    for (File file : files) {
                        String fileDate = dateByFile.get(file);
                        if(file.getAbsolutePath().contains("Media") && file.getName().equals(baseName)){
                            //prefer the 1st from the original collection with exact name match
                            keepByHash.putIfAbsent(key, file);
                        }else if(file.getAbsolutePath().contains("Media")){
                            keepByHash.putIfAbsent(key, file);
                        }
                    }
                    for (File file : files) {
                        String fileDate = dateByFile.get(file);
                        if(file.getAbsolutePath().contains("Google") && file.getName().equals(baseName) && file.getAbsolutePath().contains(fileDate)){
                            keepByHash.putIfAbsent(key, file);
                        }else if(file.getAbsolutePath().contains("Google") && file.getName().equals(baseName)){
                            keepByHash.putIfAbsent(key, file);
                        }
                    }
                }

            } else if(filesByName.asMap().keySet().size()==1){


//                for (String date : filesByDate.keySet()) {
//                    if(filesByDate.containsEntry(date, file)){
//                        fileDate = date;
//                        break;
//                    }
//                }

                //if the names are same but dates are different then pick the earliest date
                for (Collection<File> files : filesByDate.asMap().values()) {
                    for (File file : files) {
                        keepByHash.putIfAbsent(key, file);
                    }
                }

            } else {

                for (Collection<File> files : filesByDate.asMap().values()) {
                    for (File file : files) {
                        if(file.getAbsolutePath().contains("Media")) {
                            keepByHash.putIfAbsent(key, file);
                        }
                    }
                    for (File file : files) {
                        keepByHash.putIfAbsent(key, file);
                    }
                }


            }

        }

    }

    /**
     * Single thread to collect all the results back into a results file
     *
     * @param event
     * @param sequence
     * @param endOfBatch
     * @throws Exception
     */
    void reduce(DuplicateCheckEvent event, long sequence, boolean endOfBatch) throws Exception {
        //log.info("collecting event: {} {}", sequence, event);
        log.info("collecting results {}/{}", sequence + 1, TOTAL.get());

        StringWriter sw = new StringWriter();

        Multimap<String, File> filesByName = event.filesByName;
        Multimap<String, File> filesByDate = event.filesByDate;
        Multimap<HashKey, File> filesByHash = event.getFilesByHash();
        Map<HashKey, File> keepByHash = event.keepByHash;


        for (Map.Entry<HashKey, Collection<File>> entry : filesByHash.asMap().entrySet()) {
            HashKey key = entry.getKey();
            Collection<File> duplicates = entry.getValue();

            if (task.isDuplicatesOnly() && duplicates.size() == 1) {
                continue; //skip this one as it is not a duplicate
            }

            for (File file : duplicates) {
                String fileDate = "";
                for (String date : filesByDate.keySet()) {
                    if(filesByDate.containsEntry(date, file)){
                        fileDate = date;
                        break;
                    }
                }
                String baseName = "";
                for (String name : filesByName.keySet()) {
                    if(filesByName.containsEntry(name, file)){
                        baseName = name;
                        break;
                    }
                }
                int keep = keepByHash.containsValue(file)?1:0;

                String line = format("%s,\"%s\",\"%s\",%s,%s", key.getFileLength() + "_" + key.getHashHex(), file, baseName, fileDate, keep);
                sw.append(line + "\n");
                System.out.println(line);

                if(keep==0 && file.getAbsolutePath().contains("Media")){
                    System.out.println("bad choice! dont delete this one: " + file);
                    //System.exit(1);
                }
            }
        }

        FileUtils.write(OUT.get(), sw.toString(), StandardCharsets.UTF_8, true);
        event.clear();

        LATCH.get().countDown();
    }

    public Multimap<Long, File> collectBySize(Collection<File> files) {
        AtomicLong counter = new AtomicLong();
        Multimap<Long, File> fileSizeMapWk = TreeMultimap.create();
        for (File file : files) {
            Long fileLength = Long.valueOf(file.length());
            boolean put = fileSizeMapWk.put(fileLength, file);
            long count = counter.incrementAndGet();
            if (count > 0 && count % 1000 == 0) {
                log.info("[" + count + "/" + files.size() + "] collecting files by length");
            }
        }
        Multimap<Long, File> fileSizeMap = TreeMultimap.create();
        for (Map.Entry<Long, Collection<File>> entry : fileSizeMapWk.asMap().entrySet()) {
            if (task.isDuplicatesOnly()) {
                //only hash files that might be duplicates
                if (entry.getValue().size() > 1) {
                    fileSizeMap.putAll(entry.getKey(), entry.getValue());
                }
            } else {
                //otherwise hash them all
                fileSizeMap.putAll(entry.getKey(), entry.getValue());
            }
        }
        return fileSizeMap;
    }

    @Data
    private static class DuplicateCheckEvent {
        Multimap<HashKey, File> filesByHash = ArrayListMultimap.create();
        Map<File, String> dateByFile = Maps.newHashMap();
        Map<HashKey, File> keepByHash = Maps.newHashMap();
        Collection<File> filesOfSameLength = new HashSet<>();
        Multimap<String, File> filesByName = TreeMultimap.create();
        Multimap<String, File> filesByDate = TreeMultimap.create();
        Long fileLength;
        HashProvider hashProvider;
        public void clear() {
            filesByHash.clear();
            filesOfSameLength.clear();
            filesByName.clear();
            filesByDate.clear();
        }
    }

    public static class PartitionedHasher implements EventHandler<DuplicateCheckEvent> {
        private final long ordinal;
        private final long numberOfConsumers;
        public PartitionedHasher(final long ordinal, final long numberOfConsumers) {
            this.ordinal = ordinal;
            this.numberOfConsumers = numberOfConsumers;
        }
        @Override
        public void onEvent(DuplicateCheckEvent event, long sequence, boolean endOfBatch) throws Exception {
            if ((sequence % numberOfConsumers) == ordinal) {
                DuplicateFileFinder.calculateHash(event, sequence, endOfBatch);
            }
        }
    }

    public static class PartitionedDateResolver implements EventHandler<DuplicateCheckEvent> {
        private final long ordinal;
        private final long numberOfConsumers;
        private final ExifTool tool;
        public PartitionedDateResolver(final long ordinal, final long numberOfConsumers) {
            this.ordinal = ordinal;
            this.numberOfConsumers = numberOfConsumers;
            this.tool = new ExifTool(STAY_OPEN);
        }
        @Override
        public void onEvent(DuplicateCheckEvent event, long sequence, boolean endOfBatch) throws Exception {
            if ((sequence % numberOfConsumers) == ordinal) {
                DuplicateFileFinder.resolveDates(event, sequence, endOfBatch, tool);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, SQLException {
        PhotoTask task = PhotoTask.builder()
                .paths(
//                        "D:\\sources\\_github\\PhotoTool\\src\\test\\resources\\test-1"
//                        "D:/Media/photos","D:/google-takeout/3rd-go/Takeout/GooglePhotos","D:/google-takeout/3rd-go/Takeout/duplicate-candidates"
                        "D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2010-09-05"
                )
                .wildcards(
                        "*.3gp", "*.AVI", "*.bmp", "*.gif", "*.HEIC", "*.jpeg",
                        "*.jpg", "*.m4v", "*.mov", "*.mp4", "*.MPG", "*.png", "*.tif"
                )
                .reportFile("test5.csv")
                .duplicatesOnly(false)
                .build();

        DuplicateFileFinder duplicateFileFinder = new DuplicateFileFinder(task);
        duplicateFileFinder.findDuplicateFilesList();


        //duplicateFileFinder.deleteFromReport(new File("build/report-v4.csv"));
        //duplicateFileFinder.deleteFromReport(new File("build/report-v5.csv"));
        //duplicateFileFinder.deleteFromReport(new File("build/report-v6.csv"));

    }


    private void deleteFromReport(File report) throws IOException, SQLException {
        ResultSet rs = new Csv().read(new FileReader(report), new String[]{"HashKey","duplicate","baseName","fileDate","keep"});
        rs.next();//skip the header row
        while (rs.next()){
            File file = new File(rs.getString("duplicate"));
            int keep = rs.getInt("keep");
            if(keep==0){
                System.out.println("deleted: " + file);
                file.delete();
            }

        }
    }


}