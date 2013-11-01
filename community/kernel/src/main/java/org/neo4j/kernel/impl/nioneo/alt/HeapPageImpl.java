package org.neo4j.kernel.impl.nioneo.alt;

class HeapPageImpl extends AbstractPage
{
    private final byte[] data;
    
    HeapPageImpl( FileWithRecords fwr, byte[] data, long startRecord, int pageSize, int recordSize )
    {
        super( fwr, startRecord, pageSize, recordSize );
        this.data = data;
    }
    
    public byte[] readRecord( long record )
    {
        byte[] recordData = new byte[recordSize()];
        System.arraycopy( data, (int) ((record - startRecord()) * recordSize()), recordData, 0, recordSize() );
        return recordData;
    }

    public void writeRecord( long record, byte[] data )
    {
        System.arraycopy( data, 0, this.data, (int) ((record - startRecord()) * recordSize()), recordSize() );
    }

    public Page copy()
    {
        byte newData[] = new byte[pageSize() * recordSize() ];
        System.arraycopy( newData, 0, data, 0, newData.length );
        return new HeapPageImpl( getFileWithRecords(), newData, startRecord(), pageSize(), recordSize() );
    }
    
    @Override
    public void force()
    {
        getFileWithRecords().write( startRecord(), data );
    }
}