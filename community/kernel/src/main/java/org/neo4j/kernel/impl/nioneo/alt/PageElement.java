package org.neo4j.kernel.impl.nioneo.alt;

public interface PageElement
{
    byte[] readRecord( long record );

    boolean writeRecord( long record, byte[] data );
    
    void force();
    boolean free();

    boolean isAllocated();
}
