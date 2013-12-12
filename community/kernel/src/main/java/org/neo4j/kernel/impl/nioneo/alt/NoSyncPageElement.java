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
