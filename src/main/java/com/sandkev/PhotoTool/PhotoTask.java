package com.sandkev.PhotoTool;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by kevin on 10/11/2016.
 */
@Data
@Builder(builderClassName = "BuilderWithDefaults")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PhotoTask {
    enum  FilterType{INCLUDE,EXCLUDE}
    String[] wildcards;
    FilterType filterType;
    boolean duplicatesOnly;
    String[] paths;
    String reportFile;
    HashProvider hashProvider;
    public static class BuilderWithDefaults{
        String[] wildcards = {"*.3gp", "*.AVI", "*.bmp", "*.gif", "*.HEIC", "*.jpeg",
                "*.jpg", "*.m4v", "*.mov", "*.mp4", "*.MPG", "*.png", "*.tif"};
        FilterType filterType = FilterType.INCLUDE;
        String[] paths;
        boolean duplicatesOnly = true;
        HashProvider hashProvider = HashType.CRC32;
        public BuilderWithDefaults wildcards(String... wildcards){
            this.wildcards = wildcards;
            return this;
        }
        public BuilderWithDefaults paths(String... paths){
            this.paths = paths;
            return this;
        }
    }

    public Collection<File> find(){
        WildcardFileFilter filter = new WildcardFileFilter(getWildcards(), IOCase.INSENSITIVE);
        Collection<File> files = new HashSet<>();
        for (String path : getPaths()) {
            System.out.print("scanning for files in " + path);
            System.out.flush();
            Collection<File> collection = FileUtils.listFiles(
                    new File(path),
                    filterType == FilterType.INCLUDE?
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
