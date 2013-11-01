package org.neo4j.kernel.impl.nioneo.alt;

public interface Page
{
    long startRecord();
    int pageSize();
    int recordSize();
    
    byte[] readRecord( long record );
    void writeRecord( long record, byte[] data );
    
    Page copy();
    void force();
}
