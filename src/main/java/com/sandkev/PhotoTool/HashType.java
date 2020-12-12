package com.sandkev.PhotoTool;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.hash.Hashing.adler32;
import static com.google.common.hash.Hashing.crc32;
import static com.google.common.hash.Hashing.crc32c;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.murmur3_128;
import static com.google.common.hash.Hashing.murmur3_32;
import static com.google.common.hash.Hashing.sha1;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.hash.Hashing.sipHash24;

public enum HashType implements HashProvider{

    MURMUR3_128(murmur3_128()),
    MURMUR3_32(murmur3_32()),
    SIPHASH24(sipHash24()),
    MD5(md5()),
    SHA1(sha1()),
    SHA256(sha256()),
    SHA512(sha512()),
    ADLER32(adler32()),
    CRC32(crc32()),
    CRC32C(crc32c());

    private final HashFunction hashFunction;
    HashType(HashFunction hashFunction) {
        this.hashFunction = hashFunction;
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }
    public String getHashHex(File file) throws IOException {
        Hasher hasher = hashFunction.newHasher();
        int STREAM_BUFFER_LENGTH = 1024 * 4;
        byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
        try(InputStream data = new FileInputStream(file)) {
            int read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);

            while (read > -1) {
                hasher.putBytes(buffer, 0, read);
                read = data.read(buffer, 0, STREAM_BUFFER_LENGTH);
            }
            return Hex.encodeHexString(hasher.hash().asBytes());
        }
    }

    @Override
    public String describe() {
        return this.name();
    }


}
