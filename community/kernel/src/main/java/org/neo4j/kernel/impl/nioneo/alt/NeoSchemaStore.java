/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.alt;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind.deserialize;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.RecordSerializer;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

import com.sun.tools.javac.util.List;


public class NeoSchemaStore extends Store
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    public static final String VERSION = NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    public static final int BLOCK_SIZE = 56; // + BLOCK_HEADER_SIZE == 64

    public static final IdType ID_TYPE = IdType.SCHEMA;
    
    public NeoSchemaStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.SCHEMA_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, true, BLOCK_SIZE );
    }

    public static Collection<DynamicRecord> allocateFrom( SchemaRule rule, RecordStore schemaStore, DynamicRecordAllocator recordAllocator )
    {
        RecordSerializer serializer = new RecordSerializer();
        serializer = serializer.append( rule );
        return NeoDynamicStore.allocateRecordsFromBytes( serializer.serialize(), 
                 Arrays.<DynamicRecord>asList( new DynamicRecord( rule.getId(), DynamicRecord.Type.STRING ) ).iterator(),
                /*NeoDynamicStore.getRecords( schemaStore, rule.getId(), RecordLoad.NORMAL ).iterator(),*/ recordAllocator );
    }


    public static Iterator<SchemaRule> loadAllSchemaRules( final Store schemaStore )
    {
        return new PrefetchingIterator<SchemaRule>()
        {
            private final long highestId = schemaStore.getIdGenerator().getHighId();
            private long currentId = 1; /*record 0 contains the block size*/
            private final byte[] scratchData = new byte[schemaStore.getRecordStore().getRecordSize()*4];

            @Override
            protected SchemaRule fetchNextOrNull()
            {
                while ( currentId <= highestId )
                {
                    long id = currentId++;
                    byte[] data = schemaStore.getRecordStore().getRecord( id );
                    DynamicRecord record = NeoDynamicStore.getRecord( id, data, RecordLoad.FORCE, DynamicRecord.Type.STRING );
                    if ( record.inUse() && record.isStartRecord() )
                    {
                        try
                        {
                            return getSchemaRule( id, scratchData, schemaStore.getRecordStore() );
                        }
                        catch ( MalformedSchemaRuleException e )
                        {
                            // TODO remove this and throw this further up
                            throw launderedException( e );
                        }
                    }
                }
                return null;
            }
        };
    }
    
    private static SchemaRule getSchemaRule( long id, byte[] buffer, RecordStore schemaStore ) throws MalformedSchemaRuleException
    {
        return readSchemaRule( id, NeoDynamicStore.getRecords( schemaStore, id, RecordLoad.NORMAL, DynamicRecord.Type.STRING ), buffer );
    }

//    private SchemaRule forceGetSchemaRule( long id, byte[] buffer ) throws MalformedSchemaRuleException
//    {
//        Collection<DynamicRecord> records = getRecords( id, RecordLoad.FORCE );
//        for ( DynamicRecord record : records )
//        {
//            ensureHeavy( record );
//        }
//        return readSchemaRule( id, records, buffer );
//    }

    public static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records )
            throws MalformedSchemaRuleException
    {
        return readSchemaRule( id, records, new byte[ BLOCK_SIZE * 4 ] );
    }

    private static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records, byte[] buffer )
            throws MalformedSchemaRuleException
    {
        ByteBuffer scratchBuffer = NeoDynamicStore.concatData( records, buffer );
        return deserialize( id, scratchBuffer );
    }
   
}
