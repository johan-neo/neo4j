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
            if ( page != null )
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
}
