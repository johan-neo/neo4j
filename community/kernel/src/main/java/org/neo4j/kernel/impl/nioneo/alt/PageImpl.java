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
import java.nio.MappedByteBuffer;

class PageImpl extends AbstractPage
{
    private final ByteBuffer buffer;
    
    PageImpl( FileWithRecords fwr, ByteBuffer buffer, long startRecord, int pageSize, int recordSize )
    {
        super( fwr, startRecord, pageSize, recordSize );
        this.buffer = buffer;
    }

    public void readRecord( long record, byte[] recordData )
    {
        assert recordData.length == recordSize();
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position( (int) ((record - startRecord()) * recordSize()) );
        duplicate.get( recordData );
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