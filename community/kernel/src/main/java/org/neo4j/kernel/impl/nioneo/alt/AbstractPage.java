package org.neo4j.kernel.impl.nioneo.alt;

abstract class AbstractPage implements Page
{
    private final FileWithRecords fwr;
    private final long startRecord;
    private final int pageSize;
    private final int recordSize;
    
    AbstractPage( FileWithRecords fwr, long startRecord, int pageSize, int recordSize )
    {
        this.fwr = fwr;
        this.startRecord = startRecord;
        this.pageSize = pageSize;
        this.recordSize = recordSize;
    }
    
    public long startRecord()
    {
        return startRecord;
    }

    public int pageSize()
    {
        return pageSize;
    }

    public int recordSize()
    {
        return recordSize;
    }
    
    public FileWithRecords getFileWithRecords()
    {
        return fwr;
    }
}