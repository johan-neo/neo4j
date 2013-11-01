package org.neo4j.kernel.impl.nioneo.alt;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

class PageImpl extends AbstractPage
{
    private final ByteBuffer buffer;
    
    PageImpl( FileWithRecords fwr, ByteBuffer buffer, long startRecord, int pageSize, int recordSize )
    {
        super( fwr, startRecord, pageSize, recordSize );
        this.buffer = buffer;
    }

    public byte[] readRecord( long record )
    {
        byte[] recordData = new byte[recordSize()];
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position( (int) ((record - startRecord()) * recordSize()) );
        duplicate.get( recordData );
        return recordData;
    }

    public void writeRecord( long record, byte[] data )
    {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position( (int) ((record - startRecord()) * recordSize()) );
        duplicate.put( data );
    }

    public Page copy()
    {
        return new PageImpl( getFileWithRecords(), buffer, startRecord(), pageSize(), recordSize() );
    }
    
    @Override
    public void force()
    {
        if ( buffer instanceof MappedByteBuffer )
        {
            ((MappedByteBuffer) buffer).force();
        }
        else
        {
            getFileWithRecords().write( startRecord(), buffer );
        }
    }
}