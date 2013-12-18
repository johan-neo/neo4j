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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
    
    public PagedFileWithRecords( FileWithRecords fwr, int targetPageSize, PageType type, PageSynchronization refSync )
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
        PageElement pageElement = pages[element];
        pageElement.getHits().incrementCount( pageElement.isAllocated() );
        return pageElement;
    }

    @Override
    public void getRecord( long record, byte[] data )
    {
        PageElement pageElement = getPageElement( record );
        if (!pageElement.readRecord( record, data ) )
        {
            fwr.read( record, 1, data );
        }
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
        // long memoryUsed = 0;
        for ( int i = 0; i < pages.length; i++ )
        {
            Page page = null;
            /*long startRecord = i * pageSizeRecords;
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
            }*/
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

    private Page createPage( long startRecord )
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
            System.out.println( t.getMessage() );
            return null;
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
        if ( nrOfPages < pages.length )
        {
            return;
        }
        if ( nrOfPages < 1 ) 
        {
            throw new IllegalStateException( "Unable to expand, to many pages? current page count=" + pages.length + " new page count=" + nrOfPages );
        }
        PageElement[] newPages = new PageElement[nrOfPages];
        // long memoryUsed = 0;
        for ( int i = 0; i < pages.length; i++ )
        {
//            if ( pages[i].isAllocated() )
//            {
//                memoryUsed += pageSizeBytes;
//            }
            newPages[i] = pages[i];
        }
        for ( int i = pages.length; i < nrOfPages; i++ )
        {
            Page page = null;
/*            long startRecord = i * pageSizeRecords;
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
            }*/
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
    
    @Override
    public HitStat sweep( Sweeper sweeper )
    {
        if ( closed ) 
        {
            return new HitStat( 0, 0, 0, 0 );
        }
        long memoryUsed = 0;
        long memoryNeeded = 0;
        PageElement[] currentPages = pages;
        long missCount = 0;
        long hitCount = 0;
        List<SortPageElement> pagesToFree = new ArrayList<SortPageElement>();
        List<SortPageElement> pagesToAllocate = new ArrayList<SortPageElement>();
        
        for ( int i = 0; i < currentPages.length; i++  )
        {
            PageElement pageElement = currentPages[i];
            if ( pageElement.isAllocated() ) 
            {
                int count = pageElement.getHits().getCount();
                hitCount += count;
                if ( count > 0 )
                {
                    memoryNeeded += pageSizeBytes;
                }
                memoryUsed += pageSizeBytes;
                pagesToFree.add( new SortPageElement(pageElement, pageElement.getHits().getCount(), i ) );
            }
            else
            {
                int count = pageElement.getHits().getCount();
                missCount += count;
                if ( count > 0 )
                {
                    memoryNeeded += pageSizeBytes;
                }
                pagesToAllocate.add( new SortPageElement( pageElement, pageElement.getHits().getCount(), i ) );
            }
            
            pageElement.getHits().decrementCount( pageElement.isAllocated() );
        }
        HitStat stat = new HitStat( hitCount, missCount, memoryUsed, memoryNeeded );
        long memoryDelta = sweeper.getMemoryDelta( stat );
        
        pageInAndOut( pagesToFree, pagesToAllocate, memoryDelta );
        return stat;
    }

    private void pageInAndOut( List<SortPageElement> pagesToFree, List<SortPageElement> pagesToAllocate,
            long memoryDelta )
    {
        Collections.sort( pagesToFree, new Comparator<SortPageElement>()
        {
            @Override
            public int compare( SortPageElement o1, SortPageElement o2 )
            {
                return o1.getHit() - o2.getHit();
            }
            
        } );
        Collections.sort( pagesToAllocate, new Comparator<SortPageElement>()
        {
            @Override
            public int compare( SortPageElement o1, SortPageElement o2 )
            {
                return o2.getHit() - o1.getHit();
            }
            
        } );
        int pagesAllocatedCount = 0;
        int pagesFreedCount = 0;
        if ( memoryDelta < 0 )
        {
            while ( memoryDelta < 0 && pagesFreedCount < pagesToFree.size() )
            {
                pagesToFree.get( pagesFreedCount++ ).getPage().free();
                memoryDelta += pageSizeBytes;
            }
        }
        else if ( memoryDelta > 0 )
        {
            while ( memoryDelta > 0 && pagesAllocatedCount < pagesToAllocate.size() )
            {
                SortPageElement sortPageElement = pagesToAllocate.get( pagesAllocatedCount++ );
                int startRecord = sortPageElement.index * pageSizeRecords;
                PageElement pageElement = sortPageElement.pageElement;
                Page newPage = createPage( startRecord );
                if ( newPage == null )
                {
                    // failed to allocate
                    return;
                }
                if ( pageElement.allocate( newPage ) )
                {
                    memoryDelta -= pageSizeBytes;
                }
            }
        }
        else
        {
            while ( pagesFreedCount < pagesToFree.size() && pagesAllocatedCount < pagesToAllocate.size() )
            {
                SortPageElement toFree = pagesToFree.get(  pagesFreedCount++ ); 
                SortPageElement toAllocate = pagesToAllocate.get( pagesAllocatedCount++ );
                int toFreeScore = toFree.getHit() * 2;
                if ( toFreeScore < 0 )
                {
                    toFreeScore = Integer.MAX_VALUE;
                }
                int toAllocateScore = toAllocate.getHit();
                if ( toAllocateScore < 0 )
                {
                    toAllocateScore = Integer.MAX_VALUE;
                }
                if ( toFree.getHit() > toAllocate.getHit() )
                {
                    break;
                }
            }
        }
    }
    
    private static class SortPageElement
    {
        private final PageElement pageElement;
        private final int hit;
        private final int index;
        
        public SortPageElement( PageElement pageElement, int hit, int index )
        {
            this.pageElement = pageElement;
            this.hit = hit;
            this.index = index;
        }
        
        PageElement getPage()
        {
            return pageElement;
        }
        
        int getHit()
        {
            return hit;
        }
        
        @Override
        public boolean equals( Object o )
        {
            if ( o instanceof SortPageElement )
            {
                return ((SortPageElement) o).getHit() == getHit();
            }
            return false;
        }
    }

    @Override
    public long getStoreSizeInBytes()
    {
        return pages.length * pageSizeBytes;
    }
}
