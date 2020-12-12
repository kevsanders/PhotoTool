package com.sandkev.PhotoTool;

//import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class PhotoTaskResourceTest {
    @Test
    void canRunPhotoTask() throws IOException {
        PhotoTaskResource photoTaskResource = new PhotoTaskResource();
        photoTaskResource.runTask("D:/Media/photos");
        photoTaskResource.runTask("D:/google-takeout/3rd-go/Takeout/GooglePhotos");

    }

}