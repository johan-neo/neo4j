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

import java.util.concurrent.atomic.AtomicReference;

class AtomicPageElement implements PageElement
{
    final AtomicReference<Page> page;
    volatile boolean isDirty = false;

    AtomicPageElement( Page page )
    {
        this.page = new AtomicReference<Page>( page );
    }

    public byte[] readRecord( long record )
    {
        // no guard against concurrent writes on same record use:
        // return page.get().readRecord( record );
        
        // re-read any read that happened concurrently with a write
        byte[] data;
        Page pageReadFrom;
        do
        {
            pageReadFrom = page.get();
            if ( pageReadFrom != null )
            {
                data = pageReadFrom.readRecord( record );
            }
            else
            {
                return null;
            }
        }
        while ( pageReadFrom != page.get() );
        return data;
    }

    public boolean writeRecord( long record, byte[] data )
    {
        Page oldPage, newPage;
        do
        {
            oldPage = page.get();
            if ( oldPage != null )
            {
                newPage = oldPage.copy();
                newPage.writeRecord( record, data );
                isDirty = true;
            }
            else
            {
                return false;
            }
        }
        while ( !page.compareAndSet( oldPage, newPage ) );
        return true;
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
    public boolean isAllocated()
    {
        return page.get() != null;
    }
}
