package org.neo4j.kernel.impl.nioneo.alt;

public interface RecordStore
{
    public byte[] getRecord( long record );
    public void writeRecord( long record, byte[] data );
    
    public int getRecordSize();
    
    public void close();
    public void force();

    public void allocatePages( long amountBytes );
    public void freePages( long amountBytes );
    public void writeOutDirtyPages();
    
    public long getHighestPossibleIdInUse();
}
