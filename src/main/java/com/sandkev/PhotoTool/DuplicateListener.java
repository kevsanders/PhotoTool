package com.sandkev.PhotoTool;

import java.io.File;
import java.util.Collection;

public interface DuplicateListener {
    void duplicateEvent(HashKey key, Collection<File> file);
}
