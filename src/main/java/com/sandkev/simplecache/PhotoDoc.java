package com.sandkev.simplecache;

import lombok.Builder;
import lombok.Data;

import java.io.File;

/**
 * Created by kevin on 03/04/2019.
 */
@Data
@Builder
public class PhotoDoc {
    File file;
    long length;
    String name;
    String date;
    String folder;
    String crc32;
    int keepFlag;

/*
    public static PhotoDocBuilder builder(){
        return new CustomBuilder();
    }

    private static class CustomBuilder extends PhotoDocBuilder {
        private File file;
        public CustomBuilder file(String file){
            this.file = new File(file);
            return this;
        }
    }
*/
}
