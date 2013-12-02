package org.neo4j.kernel.impl.nioneo.alt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;

public class FileWithRecords
{
    private final String name;
    private final FileChannel fileChannel;
    private final int recordSize;
    
    public FileWithRecords( String name, FileChannel fileChannel, int recordSize )
    {
        if ( recordSize < 1 )
        {
            throw new IllegalArgumentException( "RecordSize: " + recordSize );
        }
        this.name = name;
        this.recordSize = recordSize;
        this.fileChannel = fileChannel;
    }

    public long getFileSize()
    {
        try
        {
            return fileChannel.size();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public int getRecordSize()
    {
        return recordSize;
    }

    public long getNrOfRecords()
    {
        return getFileSize() / recordSize;
    }

    public byte[] read( long fromRecord, int nrOfRecords )
    {
        int length = nrOfRecords * recordSize;
        byte[] data = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap( data );
        read( fromRecord, nrOfRecords, buffer );
        return data;
    }

    public void read( long fromRecord, int nrOfRecords, ByteBuffer intoBuffer )
    {
        if ( nrOfRecords < 1 )
        {
            throw new IllegalArgumentException( "Number of records to read: " + nrOfRecords );
        }
        long startPos = fromRecord * recordSize;
        if ( startPos < 0 )
        {
            throw new IllegalArgumentException( "Start record " + fromRecord );
        }
        int bytesToRead = intoBuffer.remaining();
        long fileLength = getFileSize();
        try
        {
            int read = fileChannel.read( intoBuffer, startPos );
            if ( read == -1 )
            {
                read = 0;
            }
            if ( startPos + bytesToRead > fileLength && intoBuffer.hasRemaining() )
            {
                read += intoBuffer.remaining();
                // read += fileChannel.write( intoBuffer, getFileSize() );
            }
            if ( read < bytesToRead )
            {
                throw new UnderlyingStorageException( "Only read " + read + " expected " + bytesToRead );
            }
            intoBuffer.flip();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public ByteBuffer map( long startRecord, int nrOfRecords ) throws IOException
    {
        if ( startRecord < 0 )
        {
            throw new IllegalArgumentException( "Illegal start record: " + startRecord );
        }
        if ( nrOfRecords < 1 )
        {
            throw new IllegalArgumentException( "Number of records to read: " + nrOfRecords );
        }
        long startPos = startRecord * recordSize;
        int length = nrOfRecords * recordSize;
        return fileChannel.map( MapMode.READ_WRITE, startPos, length );
    }

    public String getName()
    {
        return name;
    }

    public void write( long startRecord, byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        write( startRecord, buffer );
    }

    public void write( long startRecord, ByteBuffer fromBuffer )
    {
        long startPos = startRecord * recordSize;
        int length = fromBuffer.remaining();
        if ( startPos < 0 )
        {
            throw new IllegalArgumentException( "Start record " + startRecord );
        }
        try
        {
            int written = fileChannel.write( fromBuffer, startPos );
            if ( written != length )
            {
                throw new UnderlyingStorageException( "Only wrote " + written + " expected " + length );
            }
            fromBuffer.flip();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public void force()
    {
        try
        {
            fileChannel.force( false );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }
}
