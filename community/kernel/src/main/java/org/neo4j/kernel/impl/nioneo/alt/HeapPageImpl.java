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

class HeapPageImpl extends AbstractPage
{
    private final byte[] data;
    
    HeapPageImpl( FileWithRecords fwr, byte[] data, long startRecord, int pageSize, int recordSize )
    {
        super( fwr, startRecord, pageSize, recordSize );
        this.data = data;
    }
    
    public void readRecord( long record, byte[] recordData )
    {
        assert recordData.length == recordSize();
        System.arraycopy( data, (int) ((record - startRecord()) * recordSize()), recordData, 0, recordSize() );
    }

    public void writeRecord( long record, byte[] data )
    {
        System.arraycopy( data, 0, this.data, (int) ((record - startRecord()) * recordSize()), recordSize() );
    }

    public Page copy()
    {
        byte newData[] = new byte[pageSize() * recordSize() ];
        System.arraycopy( newData, 0, data, 0, newData.length );
        return new HeapPageImpl( getFileWithRecords(), newData, startRecord(), pageSize(), recordSize() );
    }
    
    @Override
    public void force()
    {
        getFileWithRecords().write( startRecord(), data );
    }
}