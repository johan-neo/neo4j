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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class AtomicPageElement implements PageElement
{
    final AtomicInteger nextVersion = new AtomicInteger();
    final AtomicInteger currentVersion = new AtomicInteger();
    final AtomicBoolean spinLockWriterWriting = new AtomicBoolean( false );
    
    final AtomicReference<Page> page;
    
    volatile boolean isDirty = false;
    private Hits hits = new Hits();
    
    AtomicPageElement( Page page )
    {
        this.page = new AtomicReference<Page>( page );
    }

    public boolean readRecord( long record, byte[] data )
    {
        int current; 
        boolean toReturn = false;
        do
        {
            current = currentVersion.get();
            Page pageToReadFrom = page.get();
            if ( pageToReadFrom != null )
            {
                pageToReadFrom.readRecord( record, data );
                toReturn = true;
            }
        } while ( current != nextVersion.get() );
        return toReturn;
    }

    public boolean writeRecord( long record, byte[] data )
    {
        while ( !spinLockWriterWriting.compareAndSet( false, true ) );
        try
        {
            int versionToSet;
            boolean wroteToPage;
//            do
//            {
                wroteToPage = false;
                versionToSet = nextVersion.incrementAndGet();
                Page realPage = page.get();
                if ( realPage != null )
                {
                    realPage.writeRecord( record, data );
                    isDirty = true;
                    wroteToPage = true;
                }
                currentVersion.set( versionToSet );
 //           } while ( nextVersion.get() != versionToSet );
            return wroteToPage;
        }
        finally
        {
            spinLockWriterWriting.set( false );
        }
    }

    @Override
    public boolean free()
    {
        Page oldPage;
        do
        {
            oldPage = page.get();
            if ( oldPage != null )
            {
                oldPage.force();
            }
        } 
        while ( oldPage != null && !page.compareAndSet( oldPage, null ) );
        if ( oldPage != null )
        {
            return true;
        }
        return false;
    }

    @Override
    public void force()
    {
        if ( !isDirty )
        {
            return;
        }
        Page oldPage = page.get();
        isDirty = false;
        if ( oldPage != null )
        {
            oldPage.force();
        }
    }

    @Override
    public boolean allocate( Page newPage )
    {
        return page.compareAndSet( null, newPage );
    }
    
    
    @Override
    public boolean isAllocated()
    {
        return page.get() != null;
    }

    @Override
    public Hits getHits()
    {
        return hits;
    }
}
