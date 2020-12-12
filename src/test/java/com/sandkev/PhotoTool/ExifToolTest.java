package com.sandkev.PhotoTool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.sandkev.PhotoTool.ExifTool.Feature.*;
import static org.junit.jupiter.api.Assertions.*;

class ExifToolTest {

    private ExifTool tool;

    @BeforeEach
    void setUp() {
		System.setProperty("exiftool.path", "D:/Tools/bin/exiftool.exe");
		System.setProperty("exiftool.debug", "false");
		System.setProperty("exiftool.processCleanupDelay", "0");
        tool = new ExifTool(STAY_OPEN);
        //tool = new ExifTool();
    }

    @Test
    void canGetTags() throws IOException {
        PhotoTask task = PhotoTask.builder().paths("src/test/resources").build();
        //PhotoTask task = PhotoTask.builder().paths("D:/google-takeout/3rd-go/Takeout/GooglePhotos").build();
        ExifTool.Tag[] TAGS = new ExifTool.Tag[]{ExifTool.Tag.DATE_TIME_ORIGINAL, ExifTool.Tag.CREATE_DATE, ExifTool.Tag.MODIFY_DATE};
        Collection<File> images = task.find();
        for (File image: images) {
            Map<ExifTool.Tag, String> imageMeta = tool.getImageMeta(image, TAGS);
            System.out.println(image + ": " + imageMeta);
        }
        tool.close();
    }
    


}