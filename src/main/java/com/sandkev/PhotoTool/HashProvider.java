package com.sandkev.PhotoTool;

import java.io.File;
import java.io.IOException;

public interface HashProvider {
    String getHashHex(File file) throws IOException;
    String describe();
}
