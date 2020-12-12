package com.sandkev.simplecache;

/**
 * Created by kevin on 31/03/2019.
 */
public class IndexWriterException extends RuntimeException {
    public IndexWriterException(String message) {
        super(message);
    }

    public IndexWriterException(String message, Throwable e) {
        super(message, e);
    }

    public IndexWriterException(Throwable e) {
        super(e);
    }
}
