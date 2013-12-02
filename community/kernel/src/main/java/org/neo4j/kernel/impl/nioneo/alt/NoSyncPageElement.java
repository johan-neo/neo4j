package org.neo4j.kernel.impl.nioneo.alt;

class NoSyncPageElement implements PageElement
{
    private Page page;
    // non volatile because we do not want that
    private boolean isDirty;

    NoSyncPageElement( Page page )
    {
        this.page = page;
    }

    public byte[] readRecord( long record )
    {
        if ( page != null )
        {
            return page.readRecord( record );
        }
        return null;
    }

    public boolean writeRecord( long record, byte[] data )
    {
        if ( page != null )
        {
            isDirty = true;
            page.writeRecord( record, data );
            return true;
        }
        return false;
    }

    @Override
    public boolean free()
    {
        force();
        if ( page != null )
        {
            page = null;
            return true;
        }
        return false;
    }

    @Override
    public void force()
    {
        // no sync here because we do not want it
        if ( isDirty && page != null )
        {
            page.force();
        }
        isDirty = false;
    }

    @Override
    public boolean isAllocated()
    {
        return page != null;
    }
}
