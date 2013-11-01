package org.neo4j.kernel.impl.nioneo.alt;

import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;

public class PagedFileWithRecords implements RecordStore
{
    private final FileWithRecords fwr;
    private final int pageSizeBytes;
    private final int pageSizeRecords;
    private final PageType pageType;
    private final PageSynchronization pageReferenceSync;
    private volatile PageElement[] pages;
    private boolean closed = false;

    public PagedFileWithRecords( FileWithRecords fwr, int targetPageSize, PageType type, PageSynchronization refSync,
            long initialMemory )
    {
        this.fwr = fwr;
        int recordSize = fwr.getRecordSize();
        if ( targetPageSize < recordSize )
        {
            throw new IllegalArgumentException( "Target page size to small " + targetPageSize );
        }
        this.pageSizeBytes = (targetPageSize / recordSize) * recordSize;
        this.pageSizeRecords = pageSizeBytes / recordSize;
        int nrOfPages = (int) (((fwr.getFileSize() / pageSizeBytes) + 1) * 1.1f);
        this.pageType = type;
        this.pageReferenceSync = refSync;
        this.pages = new PageElement[nrOfPages];
        setupPages( initialMemory );
    }

    private PageElement getPageElement( long record )
    {
        int element = (int) (record / pageSizeRecords);
        if ( element >= pages.length )
        {
            expand( (int) (element * 1.1f) );
        }
        return pages[(int) (record / pageSizeRecords)];
    }

    @Override
    public byte[] getRecord( long record )
    {
        PageElement pageElement = getPageElement( record );
        byte[] data = pageElement.readRecord( record );
        if ( data == null )
        {
            data = fwr.read( record, 1 );
        }
        return data;
    }

    @Override
    public void writeRecord( long record, byte[] data )
    {
        PageElement pageElement = getPageElement( record );
        if ( !pageElement.writeRecord( record, data ) )
        {
            fwr.write( record, data );
        }
    }

    private void setupPages( long initalMemory )
    {
        long memoryUsed = 0;
        for ( int i = 0; i < pages.length; i++ )
        {
            long startRecord = i * pageSizeRecords;
            Page page = null;
            if ( memoryUsed + pageSizeBytes <= initalMemory )
            {
                try
                {
                    page = createPage( startRecord );
                }
                catch ( UnableToMapPageException e )
                {
                    // make sure we do not try to allocate more memory
                    memoryUsed = initalMemory;
                    // log this somehow
                    // TODO:
                    System.out.println( fwr.getName() + " " + e.getMessage() );
                }
                memoryUsed += pageSizeBytes;
            }
            switch ( pageReferenceSync )
            {
            case ATOMIC:
                pages[i] = new AtomicPageElement( page );
                break;
            case NONE:
                pages[i] = new NoSyncPageElement( page );
                break;
            default:
                throw new IllegalArgumentException( "Invalid page synchronization " + pageReferenceSync );
            }
        }
    }
    
    private void nullPages()
    {
        for ( int i = 0; i < pages.length; i++ )
        {
            pages[i].free();
        }
    }

    private Page createPage( long startRecord ) throws UnableToMapPageException
    {
        try
        {
            switch ( pageType )
            {
            case MEMORY_MAPPED:
                return new PageImpl( fwr, fwr.map( startRecord, pageSizeRecords ), startRecord, pageSizeRecords,
                        fwr.getRecordSize() );
            case HEAP:
                ByteBuffer heapBuffer = ByteBuffer.wrap( fwr.read( startRecord, pageSizeRecords ) );
                return new PageImpl( fwr, heapBuffer, startRecord, pageSizeRecords, fwr.getRecordSize() );
            case DIRECT:
                ByteBuffer directBuffer = ByteBuffer.allocateDirect( pageSizeBytes );
                fwr.read( startRecord, pageSizeRecords, directBuffer );
                return new PageImpl( fwr, directBuffer, startRecord, pageSizeRecords, fwr.getRecordSize() );
            default:
                throw new IllegalArgumentException( "Invalid page type " + pageType );
            }
        }
        catch ( Throwable t )
        {
            // TODO: track amount of errors here and log every X failure
            throw new UnableToMapPageException( t );
        }
    }

    @Override
    public synchronized void close()
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
        closed = true;
        nullPages();
        fwr.force();
    }

    @Override
    public synchronized void force()
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
        writeOutDirtyPages();
        fwr.force();
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    private synchronized void expand( int nrOfPages )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    public long getNrOfRecords()
    {
        return fwr.getNrOfRecords();
    }

    @Override
    public synchronized void allocatePages( long amountBytes )
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public synchronized void freePages( long amountBytes )
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public synchronized void writeOutDirtyPages()
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        long fwrCount = fwr.getNrOfRecords();
        long pageCount = pages.length * pageSizeRecords;
        return Math.max( fwrCount, pageCount );
    }
}
