package com.sandkev.PhotoTool;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class DataLoaderTest {

    @Test
    void canLoadData() throws SQLException {
//        DataLoader.load("build/sample.csv");
        DataLoader.load("build/file2Hex.csv");
    }

}