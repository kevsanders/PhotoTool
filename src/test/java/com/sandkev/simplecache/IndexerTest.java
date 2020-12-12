package com.sandkev.simplecache;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

class IndexerTest {


    @Test
    void canFind() throws IOException {
        String storagePath = "src/main/resources/static/index";
        Indexer indexer = new Indexer(storagePath);

//        Collection<PhotoDoc> keys = indexer.findKeys("file:D:\\google-takeout\\3rd-go\\Takeout\\GooglePhotos\\2018-12-08\\_2fstorage_2femulated_2f0_2fWhatsApp_2fMedia_2f(4).mp4", 0, 10);
        //Collection<PhotoDoc> keys = indexer.findKeys("name:IMG_3197.MOV and length:3758", 0, 10);
        Collection<File> keys;// = indexer.findKeys("D:\\Media\\photos\\201607_Kevin'sPhone\\IMG_3197.MOV", 0, 10);
        keys = indexer.findKeys("*3732.JPG", 0, 10);

        System.out.println(keys);

//[D:\google-takeout\3rd-go\Takeout\GooglePhotos\2007-08-02\IMG_3901.JPG, D:\Media\photos\200707_cuhon\IMG_3901.JPG]


//        keys = indexer.findKeys("*", 0, 100000);
//        for (File key : keys) {
//            System.out.println(key);
//        }
    }


}