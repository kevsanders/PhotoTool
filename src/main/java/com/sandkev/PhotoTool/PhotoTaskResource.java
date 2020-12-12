package com.sandkev.PhotoTool;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.io.input.ReaderInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class PhotoTaskResource {
    protected final static char[] hexArray = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public void runTask(String task) throws IOException {
        PhotoTask photoTask = PhotoTask.builder().paths("","").build();
        processTask(photoTask);
    }

    private void processTask(PhotoTask photoTask) throws IOException {

        WildcardFileFilter filter = new WildcardFileFilter(photoTask.getWildcards());
        Collection<File> files = new HashSet<>();
        for (String path : photoTask.getPaths()) {
            System.out.print("scanning for files in " + path);
            Collection<File> collection = FileUtils.listFiles(
                    new File(path),
                    photoTask.filterType== PhotoTask.FilterType.INCLUDE?
                            filter : new NotFileFilter(filter),
                    TrueFileFilter.INSTANCE
            );
            System.out.println(". found " + collection.size() + " files");
            files.addAll(
                    collection
            );
        }

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CompletionService<String> completionService = new ExecutorCompletionService(executorService);

        //Multimap<Long, File> bySize = new DuplicateFileFinder("MD5").collectBySize(files);

        AtomicLong counter = new AtomicLong();
        long start = System.currentTimeMillis();
        for (File file : files) {
            completionService.submit(() -> {
                String hex = hashFile(file);
                long count = counter.incrementAndGet();
                if(count>0 && count % 1000 == 0) {
                    System.out.println("[" + count +"/" + files.size() + "]" + hex + " " + file.getAbsoluteFile());
                }
                return hex;
            });
        }

        System.out.println("found " + files.size() + " files " + (System.currentTimeMillis() - start));
        Map<File,String> file2Hex = new HashMap<>();
        File out = new File("build/file2Hex.csv");
        if(!out.exists()){
            FileUtils.write(out, "file,hex\n", StandardCharsets.UTF_8);
        }
        for (File file : files) {
            try {
                String hex = completionService.take().get();
                //file2Hex.put(file, hex);
                FileUtils.write(out, file + ", " + hex + "\n", StandardCharsets.UTF_8, true);
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

/*
        boolean duplicateCheck = true;
        if(duplicateCheck) {
            Map<String, Set<File>> filesByHex = new HashMap<>();
            for (Map.Entry<File, String> ent : file2Hex.entrySet()) {
                File file = ent.getKey();
                String key = ent.getValue();
                Set<File> filesForHex = filesByHex.get(key);
                if (filesForHex == null) {
                    filesByHex.put(key, filesForHex = new HashSet<>());
                }
                filesForHex.add(file);
            }
            Map<String, Set<File>> dups = new HashMap();
            for (Map.Entry<String, Set<File>> entry : filesByHex.entrySet()) {
                String key = entry.getKey();
                Set<File> fileSet = entry.getValue();
                if (fileSet.size() > 1) {
                    dups.put(key, fileSet);
                }
            }


            System.out.println("found " + files.size() + " files " + (System.currentTimeMillis() - start));
            for (Map.Entry<String, Set<File>> entry : dups.entrySet()) {
                System.out.println(entry);
            }
            System.out.println(dups.size() + " duplicates found");
        }
*/

    }

    private String hashFile(File file) throws IOException {
        //found 12260 files 129899  *.JPG <-- 1
        //found 12260 files 76215 <-- 3
        //found 12260 files 63327
        //found 13340 files 61248
        //found 1085 files 3380 <-- 1
        //found 1085 files 1614 <-- 3
        //found 1085 files 1613 <-- 5
        //found 1085 files 1393 <-- 10

/*
        Map<String,String> hashes = new HashMap<>();
        byte[] input = FileUtils.readFileToByteArray(file);
//        String crc32 = Hashing.crc32().hashBytes(input).toString();
//        hashes.put("crc32", crc32);
        //String md5 = Hashing.md5().hashBytes(input).toString();
        //hashes.put("md5", md5);
        String murmur3_128 = Hashing.murmur3_128().hashBytes(input).toString();
        hashes.put("murmur3_128", murmur3_128);
        hashes.put("length", String.valueOf(file.length()));
*/

        String hex = new Sha1HashProvider().getHashHex(file);
        //byte[] input = FileUtils.readFileToByteArray(file);
        //String hex = Hashing.murmur3_128().hashBytes(input).toString();

        return file.length() + "_" + hex;

        //found 1085 files 67508 <-- 3
        //return checksum(new FileReader(file));

        //return DigestUtils.sha1Hex(FileUtils.readFileToByteArray(file));

    }


    public static String checksum(Reader reader) throws IOException {
        InputStream fis = null;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA1");

            fis = new ReaderInputStream(reader);
            byte[] data = new byte[1024 * 4];
            int read;
            while ((read = fis.read(data)) != -1) {
                digest.update(data, 0, read);
            }
            return Hex.encodeHexString(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } finally {
            if(fis!=null)fis.close();
        }
    }
    private String digest(MessageDigest digest, InputStream data) throws IOException {
        int STREAM_BUFFER_LENGTH = 1024 * 4;
        byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
        int read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);

        while (read > -1) {
            digest.update(buffer, 0, read);
            read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);
        }

        return Hex.encodeHexString(digest.digest());
        //return digest.digest();
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

}
