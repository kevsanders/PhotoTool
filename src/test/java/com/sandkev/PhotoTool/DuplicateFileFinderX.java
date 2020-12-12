package com.sandkev.PhotoTool;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the duplicate files using md5/sha1 hashing, which is used only for the sizes which are of same size.
 *
 * @author HemantSingh
 *
 */
public class DuplicateFileFinderX {

    private static final Logger log = LoggerFactory.getLogger(DuplicateFileFinderX.class);

    private HashProvider hashProvider;
    // Used only for logging purpose.
    private String hashingAlgo;

    public DuplicateFileFinderX(String hashingAlgo) {
        this.hashingAlgo = hashingAlgo;
        if ("SHA1".equalsIgnoreCase(hashingAlgo)) {
            hashProvider = new Sha1HashProvider();
        } else if ("MD5".equalsIgnoreCase(hashingAlgo)) {
            hashProvider = new Md5HashProvider();
        } else if ("BOTH".equalsIgnoreCase(hashingAlgo)) {
            hashProvider = new Md5PlusSha1HashProvider();
        } else {
            throw new RuntimeException("Unsupported hashing algorithm:" + hashingAlgo + " Please use either SHA1 or MD5.");
        }
    }

    /**
     * This API returns the list of duplicate files reference.
     *
     * @param files
     *            - List of all the files which we need to check for duplicates.
     * @param duplicateListener
     * @return It returns the list which contains list of duplicate files for
     *         e.g. if a file a.JPG have 3 copies then first element in the list
     *         will be list with three references of File reference.
     */
    public Multimap<HashKey, File> findDuplicateFilesList(Collection<File> files, DuplicateListener duplicateListener) {

        Multimap<HashKey, File> finalListOfDuplicates = TreeMultimap.create(); //if the files are in date folders then this helps to pick the best one
        // First create the map for the file size and file reference in the array list.
        Multimap<Long, File> fileSizeMap = collectBySize(files);

        log.info(fileSizeMap.size() + " files will go thru " + hashingAlgo + " hash check to verify if they are duplicate.");

        // Now we will scan the potential duplicate files, and eliminate false positives using md5 hash check.
        AtomicLong counter = new AtomicLong();
        for (Map.Entry<Long, Collection<File>> entry : fileSizeMap.asMap().entrySet()) {
            Long fileLength = entry.getKey();
            Multimap<String, File> candidates = ArrayListMultimap.create();
            for (File file : entry.getValue()) {
                try {
                    String md5Hex = hashProvider.getHashHex(file);
                    candidates.put(md5Hex, file);
                } catch (IOException e) {
                    log.error("Failed to hash file {}", file, e);
                }
            }
            for (Map.Entry<String, Collection<File>> dupEntry : candidates.asMap().entrySet()) {
                if(dupEntry.getValue().size()>1){
                    HashKey key = new HashKey(fileLength, dupEntry.getKey(), hashingAlgo);
                    //finalListOfDuplicates.putAll(key, entry.getValue());
                    //duplicateListener.duplicateEvent(key, entry.getValue());
                    duplicateListener.duplicateEvent(key, dupEntry.getValue());
                    log.info("[" + counter.getAndIncrement() + "/" + fileSizeMap.size() + "]Duplicates found: {} {}", key,  dupEntry.getValue());
                }
            }
        }

        return finalListOfDuplicates;
    }

    public Multimap<Long, File> collectBySize(Collection<File> files) {
        AtomicLong counter = new AtomicLong();
        Multimap<Long, File> fileSizeMapWk = ArrayListMultimap.create();
        for (File file : files) {
            Long fileLength = Long.valueOf(file.length());
            boolean put = fileSizeMapWk.put(fileLength, file);
            long count = counter.incrementAndGet();
            if(count>0 && count % 1000 == 0) {
                log.info("[" + count +"/" + files.size() + "] collecting files by length");
            }
        }
        Multimap<Long, File> fileSizeMap = ArrayListMultimap.create();
        for (Map.Entry<Long, Collection<File>> entry : fileSizeMapWk.asMap().entrySet()) {
            if(entry.getValue().size() > 1){
                fileSizeMap.putAll(entry.getKey(), entry.getValue());
            }
        }
        return fileSizeMap;
    }

    abstract class HashProvider {
        abstract String getHashHex(File file) throws IOException ;
    }

    class Md5HashProvider extends HashProvider {
        String getHashHex(File file) throws IOException {
            return DigestUtils.md5Hex(new FileInputStream(file));
        }
    }
    class Sha1HashProvider extends HashProvider {
        String getHashHex(File file) throws IOException {
            return DigestUtils.sha1Hex(new FileInputStream(file));
        }
    }
    class Md5PlusSha1HashProvider extends HashProvider {
        String getHashHex(File file) throws IOException {
            String sha1 = new Sha1HashProvider().getHashHex(file);
            String md5 = new Md5HashProvider().getHashHex(file);
            return md5 + "+" + sha1;
        }
    }
}