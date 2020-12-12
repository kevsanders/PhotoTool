package com.sandkev.PhotoTool;

import lombok.Value;

import java.util.Comparator;

@Value
public class HashKey implements Comparable<HashKey>{
    Long fileLength;
    String hashHex;
    String hashAlgo;

    @Override
    public int compareTo(HashKey other) {
        return Comparator.comparing(HashKey::getFileLength)
                .thenComparing(HashKey::getHashHex)
                .thenComparing(HashKey::getHashAlgo)
                .compare(this, other);
    }

    @Override
    public String toString(){
        return fileLength + "_" + hashHex + "[" + hashAlgo + "]";
    }

}
