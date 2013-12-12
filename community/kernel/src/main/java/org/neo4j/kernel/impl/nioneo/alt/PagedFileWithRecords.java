/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
    
    private long memoryToUse = 0;

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
        this.memoryToUse = initialMemory;
        setupPages();
    }

    private PageElement getPageElement( long record )
    {
        int element = (int) (record / pageSizeRecords);
        if ( element >= pages.length )
        {
            // if element == 1 pages array length has to be size 2
            expand( (int) ((element+1) * 1.1f) );
        }
        return pages[element];
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

    private void setupPages()
    {
        long memoryUsed = 0;
        for ( int i = 0; i < pages.length; i++ )
        {
            long startRecord = i * pageSizeRecords;
            Page page = null;
            if ( memoryUsed + pageSizeBytes <= memoryToUse )
            {
                try
                {
                    page = createPage( startRecord );
                }
                catch ( UnableToMapPageException e )
                {
                    // make sure we do not try to allocate more memory
                    memoryUsed = memoryToUse;
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
        checkClosed();
        closed = true;
        nullPages();
        fwr.force();
    }

    @Override
    public synchronized void force()
    {
        checkClosed();
        writeOutDirtyPages();
        fwr.force();
    }

    private synchronized void expand( int nrOfPages )
    {
        if ( nrOfPages < 1 || nrOfPages < pages.length )
        {
            throw new IllegalStateException( "Unable to expand, to many pages? current page count=" + pages.length + " new page count=" + nrOfPages );
        }
        PageElement[] newPages = new PageElement[nrOfPages];
        long memoryUsed = 0;
        for ( int i = 0; i < pages.length; i++ )
        {
            if ( pages[i].isAllocated() )
            {
                memoryUsed += pageSizeBytes;
            }
            newPages[i] = pages[i];
        }
        for ( int i = pages.length; i < nrOfPages; i++ )
        {
            long startRecord = i * pageSizeRecords;
            Page page = null;
            if ( memoryUsed + pageSizeBytes <= memoryToUse )
            {
                try
                {
                    page = createPage( startRecord );
                }
                catch ( UnableToMapPageException e )
                {
                    // make sure we do not try to allocate more memory
                    memoryUsed = memoryToUse;
                    // log this somehow
                    // TODO:
                    System.out.println( fwr.getName() + " " + e.getMessage() );
                }
                memoryUsed += pageSizeBytes;
            }
            switch ( pageReferenceSync )
            {
            case ATOMIC:
                newPages[i] = new AtomicPageElement( page );
                break;
            case NONE:
                newPages[i] = new NoSyncPageElement( page );
                break;
            default:
                throw new IllegalArgumentException( "Invalid page synchronization " + pageReferenceSync );
            }
        }
        this.pages = newPages;
    }

    public long getNrOfRecords()
    {
        return fwr.getNrOfRecords();
    }

    @Override
    public synchronized void allocatePages( long amountBytes )
    {
        checkClosed();
        memoryToUse += amountBytes;
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public synchronized void freePages( long amountBytes )
    {
        checkClosed();
        throw new UnsupportedOperationException( "Not implemented yet" );
        // memoryToUse -= amountBytes;
    }

    @Override
    public synchronized void writeOutDirtyPages()
    {
        checkClosed();
        for ( PageElement page : pages )
        {
            page.force();
        }
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        long fwrCount = fwr.getNrOfRecords();
        long pageCount = pages.length * pageSizeRecords;
        return Math.max( fwrCount, pageCount );
    }

    @Override
    public int getRecordSize()
    {
        return fwr.getRecordSize();
    }

    private void checkClosed()
    {
        if ( closed )
        {
            throw new UnderlyingStorageException( "Closed store " + fwr.getName() );
        }
    }

}
