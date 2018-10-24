package com.hbase.day02;

public interface RowKeyGenerator {
    byte [] nextId();
}
