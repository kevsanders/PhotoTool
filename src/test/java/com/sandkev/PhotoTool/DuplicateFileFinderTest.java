package com.sandkev.PhotoTool;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

class DuplicateFileFinderTest {
    @Test
    void canFindDuplicates() throws IOException {
//        photoTaskResource.runTask("D:/Media/photos");
//        photoTaskResource.runTask("D:/google-takeout/3rd-go/Takeout/GooglePhotos");

        System.out.println("scanning for files");
        String[] wildcards = {"*.3gp","*.AVI","*.bmp","*.gif","*.HEIC","*.jpeg",
                "*.jpg","*.m4v","*.mov","*.mp4","*.MPG","*.png","*.tif"};
        ArrayList<String> paths = com.google.common.collect.Lists.newArrayList(
                "D:/Media/photos"
                ,"D:/google-takeout/3rd-go/Takeout/GooglePhotos"
                ,"D:/google-takeout/3rd-go/Takeout/duplicate-candidates"
        );
        Collection<File> files = new ArrayList<>();
        for (String path : paths) {
            files.addAll(
                    FileUtils.listFiles(
                            new File(path),
                            new WildcardFileFilter(wildcards, IOCase.INSENSITIVE),
                            TrueFileFilter.INSTANCE
                    )
            );
        }

        File out = new File("build/duplicates-all-v3.csv");
        if(!out.exists()){
            FileUtils.write(out, "HashKey,duplicate,baseName,countOfBaseNameInGroup\n", StandardCharsets.UTF_8);
        }
        DuplicateListener duplicateListener = new DuplicateListener() {
            @Override
            public void duplicateEvent(HashKey key, Collection<File> files) {
                try {
                    StringWriter sw = new StringWriter();
                    Multimap<String, File> baseNames = TreeMultimap.create();
                    Map<File, String> fileToBaseName = new HashMap<>();
                    for (File file : files) {
                        String baseName = file.getName().replace("(1)", "").replace("_edited", "");
                        baseNames.put(baseName, file);
                        fileToBaseName.put(file, baseName);
                    }
                    for (File dup : files) {
                        String baseName = fileToBaseName.get(dup);
                        String line = format("%s,%s,%s,%s", key.getFileLength() + "_" + key.getHashHex(),dup,baseName,baseNames.get(baseName).size());
                        sw.append(line + "\n");
                        System.out.println(line);
                    }
                    FileUtils.write(out, sw.toString(), StandardCharsets.UTF_8, true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        System.out.println("found " + files.size() + " files");
        new DuplicateFileFinder("BOTH").findDuplicateFilesList(files, duplicateListener);


/*
        Multimap<DuplicateFileFinder.HashKey, File> duplicateFilesList = new DuplicateFileFinder("BOTH").findDuplicateFilesList(files, duplicateListener);
        for (Map.Entry<DuplicateFileFinder.HashKey, Collection<File>> entry : duplicateFilesList.asMap().entrySet()) {
            StringWriter sw = new StringWriter();
            for (File dup : entry.getValue()) {
                sw.append(entry.getKey() + "," + dup + "\n");
            }
            FileUtils.write(out, sw.toString(), StandardCharsets.UTF_8, true);
            System.out.println(sw.toString());
        }
*/
    }

    /*

    //some notes on how to handle the differences

    1) same name different folder:
    DuplicateFileFinder.HashKey(fileLength=97860, hash=5da097bf2b72ec2bb3833983831f13cb33e18945, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-23\IMG_3353.JPG, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2018-04-04\IMG_3353.JPG]
    2016-08-23\IMG_3353.JPG <- this is the first in the list (ordered by date) and it matches the date taken in the file properties
    2018-04-04\IMG_3353.JPG <- because names match and the other date is better, we can remove this one

    2) names match (after removing "(1)" from one of them, folders match:
    DuplicateFileFinder.HashKey(fileLength=111922, hash=8b76d976e1be351814be80540867e4174bbf2178, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2009-07-11\Video001(1).3gp, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2009-07-11\Video001.3gp]
    2009-07-11\Video001(1).3gp <- this one is at the front and it matches the date in the photo
    2009-07-11\Video001.3gp <- but this is the one we want to keep

    3)false positives: more than 2 duplicates
    DuplicateFileFinder.HashKey(fileLength=130924, hash=5688d785a090d620bfbaea694e33af51a6ec19a1, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-23\IMG_3351.JPG, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2017-08-31\IMG-20170831-WA0000.jpg, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2018-04-04\IMG_3351.JPG]
    2016-08-23\IMG_3351.JPG
    2017-08-31\IMG-20170831-WA0000.jpg <- if we remove the odd-one-out this is the same as rule #1 - a second hash method should see that this file is different
    2018-04-04\IMG_3351.JPG

    4)folder name is not a proper date
    DuplicateFileFinder.HashKey(fileLength=154479, hash=efdc17f4e8470fb03c0df3ead2b1caca3b9f0e78, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-21 #2\IMG_3348.JPG, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2018-04-04\IMG_3348.JPG]
    2016-08-21 #2\IMG_3348.JPG <- the date matches the folder name (after removing " #2")
    2018-04-04\IMG_3348.JPG <- the date on this photo does not match the folder name - we remove this one

    5)folder name not generated by google
    DuplicateFileFinder.HashKey(fileLength=176172, hash=9eb762f6baabda687b0be3863878dbccb0175173, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\200804_AmeliaChristening\Baby Alia-weeks 10 _ 11 - Amelia_s Christening .jpg, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-01-09\Baby Alia-weeks 10 _ 11 - Amelia_s Christening .jpg]
    200804_AmeliaChristening\Baby Alia-weeks 10 _ 11 - Amelia_s Christening .jpg <- this folder is first by chance (date driven folder name) this is correct just because i know this is mimis christening
    2016-01-09\Baby Alia-weeks 10 _ 11 - Amelia_s Christening .jpg] <- it looks like a sensible date, but does not match the photo date (6th April 2008)

    6)combination of #1 and #2 (1:names match but folders are different)(2:name are same but for "(1)") NB not to be confused for #3 (3:more than 2 duplicates)
    DuplicateFileFinder.HashKey(fileLength=2680399, hash=491a84eae9047f77d1eb766d47744a59183ac9d3, hashAlgo=SHA1)[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-31\IMG_3380.JPG, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2018-04-04\IMG_3380(1).JPG, D:\google-takeout\3rd-go\Takeout\GooglePhotos\2018-04-04\IMG_3380.JPG]
    2016-08-31\IMG_3380.JPG <- we keep this one
    2018-04-04\IMG_3380(1).JPG <- folders match: remove "(1)"
    2018-04-04\IMG_3380.JPG <- name matches, but not folder; date does not match:

in all cases it would be useful to be able to lookup the date the photo was taken
BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
FileTime time = attrs.creationTime();

this works
exiftool -"CreateDate" D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-23\IMG_3353.JPG -csv -d %Y-%m-%d
SourceFile,CreateDate
D:/google-takeout/3rd-go/Takeout/GooglePhotos/2016-08-23/IMG_3353.JPG,2016:08:23 12:32:23

exiftool -"CreateDate" D:\google-takeout\3rd-go\Takeout\GooglePhotos\2016-08-23\IMG_3353.JPG -csv -d %Y-%m-%d -r
exiftool -"CreateDate" D:\Media -csv -d %Y-%m-%d -r > create_date_by_file.csv

exiftool -allDates D:\Media\photos -csv -d %Y-%m-%d -r > all_dates_by_file.csv
exiftool -allDates D:\google-takeout\3rd-go\Takeout\GooglePhotos -csv -d %Y-%m-%d -r > D:\google-takeout\3rd-go\Takeout\all_dates_by_file.csv

exiftool -allDates D:\google-takeout\3rd-go\Takeout\duplicate-candidates -csv -d %Y-%m-%d -r > D:\google-takeout\3rd-go\Takeout\duplicate-candidates\all_dates_by_file.csv

--after 1st round of delete
exiftool -allDates D:\google-takeout\3rd-go\Takeout -csv -d %Y-%m-%d -r > D:\google-takeout\3rd-go\Takeout\all_dates_by_file_v2.csv

exiftool -stay_open true -allDates D:\google-takeout\3rd-go\Takeout\GooglePhotos\2017-08-28 -csv -d %Y-%m-%d -r > D:\google-takeout\3rd-go\Takeout\all_dates_by_file_2017-08-28.csv

2017-08-28


     */

    @Test
    void checkDates() throws IOException {
        List<String> filePaths = Lists.newArrayList(
                "D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2016-08-23\\IMG_3353.JPG"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2018-04-04\\IMG_3353.JPG"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\200804_AmeliaChristening\\Baby Alia-weeks 10 _ 11 - Amelia_s Christening .jpg"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2007-04-07\\Video003(1).3gp"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2016-08-31\\IMG_3380.JPG"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2018-04-04\\IMG_3380(1).JPG"
                ,"D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2018-04-04\\IMG_3380.JPG"
        );
        for (String filePath : filePaths) {
            File file = new File(filePath);
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            FileTime time = attrs.creationTime(); //this date is useless
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String formatted = simpleDateFormat.format(new Date(time.toMillis()));
            System.out.println(file.getName() + " " + file.getParentFile().getName() + " " + formatted);
        }
    }

}