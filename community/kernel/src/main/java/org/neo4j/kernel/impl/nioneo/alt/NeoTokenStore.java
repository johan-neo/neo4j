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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;

/**
 * Implementation of the property store.
 */
public class NeoTokenStore extends Store
{
    // Historical type descriptor, should be called PropertyKeyTokenStore
    public static final String PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR = "PropertyIndexStore";
    public static final int PROPERTY_KEY_TOKEN_RECORD_SIZE = 1/*inUse*/ + 4/*prop count*/ + 4/*nameId*/;

    public static final String LABEL_TOKEN_TYPE_DESCRIPTOR = "LabelTokenStore";
    public static final int LABEL_TOKEN_RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    public static final String RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR = "RelationshipTypeStore";
    public static final int RELATIONSHIP_TYPE_TOKEN_RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;
    
    public static final int NAME_STORE_BLOCK_SIZE = 30;
    
    public NeoTokenStore( StoreParameter po, String fileName, IdType idType, String typeDescriptor, int recordSize )
    {
        super( new File( po.path, fileName ), po.config, idType, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, typeDescriptor, false, recordSize );
    }
   
    public static Token[] getTokens( RecordStore tokenStore, RecordStore tokenStringStore, int maxCount )
    {
        LinkedList<Token> recordList = new LinkedList<>();
        for ( int i = 0; i < maxCount; i++ )
        {
            byte[] data;
            data = tokenStore.getRecord( i );
            ByteBuffer buffer = ByteBuffer.wrap( data );
            boolean inUse = buffer.get() == Record.IN_USE.byteValue();
            if ( !inUse )
            {
                continue;
            }
            // TODO: fix this and make property token store same record size as all the others
            // we do not really use the prop count anyway
            if ( data.length == PROPERTY_KEY_TOKEN_RECORD_SIZE )
            {
                buffer.getInt();
            }
            long stringNameId = buffer.getInt();
            String name = NeoPropertyStringStore.decodeString( NeoDynamicStore.readByteArray( tokenStringStore, stringNameId, DynamicRecord.Type.STRING ) );
            recordList.add( new Token( name, i ) );
        }
        return recordList.toArray( new Token[recordList.size()] );
    }


    public static Token getToken( RecordStore tokenStore, RecordStore tokenStringStore, int id, boolean force )
    {
        byte[] data = tokenStore.getRecord( id );
        ByteBuffer buffer = ByteBuffer.wrap( data );
        boolean inUse = buffer.get() == Record.IN_USE.byteValue();
        if ( !inUse && !force )
        {
            throw new InvalidRecordException( "Token " + id + " not in use" );
        }
        // TODO: fix this and make property token store same record size as all the others
        // we do not really use the prop count anyway
        if ( data.length == PROPERTY_KEY_TOKEN_RECORD_SIZE )
        {
            buffer.getInt();
        }
        long stringNameId = buffer.getInt();
        String name = NeoPropertyStringStore.decodeString( NeoDynamicStore.readByteArray( tokenStringStore, stringNameId, DynamicRecord.Type.STRING ) );
        return new Token( name, id );
    }


    public static void updateToken( RecordStore tokenStore, RecordStore tokenNameStore, TokenRecord record, byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        if ( record.inUse() )
        {
            buffer.put( Record.IN_USE.byteValue() );
            if ( data.length == PROPERTY_KEY_TOKEN_RECORD_SIZE )
            {
                buffer.putInt( 0 );
            }
            buffer.putInt( record.getNameId() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
        }
        tokenStore.writeRecord( record.getId(), data );
        for ( DynamicRecord dynamicRecord : record.getNameRecords() )
        {
            byte[] dynamicData = NeoDynamicStore.writeToByteArray( dynamicRecord );
            tokenNameStore.writeRecord( dynamicRecord.getId(), dynamicData );
        }
    }
}