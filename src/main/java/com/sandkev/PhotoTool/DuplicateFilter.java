package com.sandkev.PhotoTool;

import lombok.Builder;
import lombok.Data;

import java.io.File;
import java.time.LocalDate;

import static java.lang.String.join;

public class DuplicateFilter {

    @Data
    @Builder
    public static class MediaInfo{
        File file;
        LocalDate date;
        HashInfo hashInfo;
    }

    @Data
    @Builder
    public static class HashInfo{
        long length;
        String cr32;
        String md5;
        String sha1;
        String murmur_128;
        String version;
        public String calcVersion(){
            return join("_", String.valueOf(length), cr32, md5, sha1, murmur_128);
        }
    }


}
