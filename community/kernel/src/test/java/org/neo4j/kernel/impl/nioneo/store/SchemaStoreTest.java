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
package org.neo4j.kernel.impl.nioneo.store;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoDynamicStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoSchemaStore;
import org.neo4j.kernel.impl.nioneo.alt.NewDynamicRecordAllocator;
import org.neo4j.test.EphemeralFileSystemRule;

public class SchemaStoreTest
{
    @Test
    public void serializationAndDeserialization() throws Exception
    {
        // GIVEN
        int propertyKey = 4;
        int labelId = 1;
        IndexRule indexRule = IndexRule.indexRule( schemaStore.getIdGenerator().nextId(), labelId, propertyKey, PROVIDER_DESCRIPTOR );

        // WHEN
        byte[] serialized = new RecordSerializer().append( indexRule ).serialize();
        IndexRule readIndexRule = (IndexRule) SchemaRule.Kind.deserialize( indexRule.getId(), wrap( serialized ) );

        // THEN
        assertEquals( indexRule.getId(), readIndexRule.getId() );
        assertEquals( indexRule.getKind(), readIndexRule.getKind() );
        assertEquals( indexRule.getLabel(), readIndexRule.getLabel() );
        assertEquals( indexRule.getPropertyKey(), readIndexRule.getPropertyKey() );
        assertEquals( indexRule.getProviderDescriptor(), readIndexRule.getProviderDescriptor() );
    }

    @Test
    public void storeAndLoadAllShortRules() throws Exception
    {
        // GIVEN
        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList(
                IndexRule.indexRule( schemaStore.getIdGenerator().nextId(), 0, 5, PROVIDER_DESCRIPTOR ),
                IndexRule.indexRule( schemaStore.getIdGenerator().nextId(), 1, 6, PROVIDER_DESCRIPTOR ),
                IndexRule.indexRule( schemaStore.getIdGenerator().nextId(), 1, 7, PROVIDER_DESCRIPTOR ) );
        for ( SchemaRule rule : rules )
        {
            storeRule( rule );
        }

        // WHEN
        Collection<SchemaRule> readRules = asCollection( NeoSchemaStore.loadAllSchemaRules( schemaStore ) );

        // THEN
        assertEquals( rules, readRules );
    }

//    ENABLE WHEN MULTIPLE PROPERTY KEYS PER INDEX RULE IS SUPPORTED
//    @Test
//    public void storeAndLoadSingleLongRule() throws Exception
//    {
//        // GIVEN
//
//        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList( createLongIndexRule( 0, 50 ) );
//        for ( SchemaRule rule : rules )
//            storeRule( rule );
//
//        // WHEN
//        Collection<SchemaRule> readRules = asCollection( store.loadAll() );
//
//        // THEN
//        assertEquals( rules, readRules );
//    }
//
//    @Test
//    public void storeAndLoadAllLongRules() throws Exception
//    {
//        // GIVEN
//        Collection<SchemaRule> rules = Arrays.<SchemaRule>asList(
//                createLongIndexRule( 0, 100 ), createLongIndexRule( 1, 6 ), createLongIndexRule( 2, 50 ) );
//        for ( SchemaRule rule : rules )
//            storeRule( rule );
//
//        // WHEN
//        Collection<SchemaRule> readRules = asCollection( store.loadAll() );
//
//        // THEN
//        assertEquals( rules, readRules );
//    }
//
//    private IndexRule createLongIndexRule( long label, int numberOfPropertyKeys )
//    {
//        long[] propertyKeys = new long[numberOfPropertyKeys];
//        for ( int i = 0; i < propertyKeys.length; i++ )
//            propertyKeys[i] = i;
//        return new IndexRule( store.nextId(), label, POPULATING, propertyKeys );
//    }

    private long storeRule( SchemaRule rule )
    {
        Collection<DynamicRecord> records = NeoSchemaStore.allocateFrom( rule, 
                schemaStore.getRecordStore(), new NewDynamicRecordAllocator( schemaStore, DynamicRecord.Type.UNKNOWN )  );
        for ( DynamicRecord record : records )
        {
            byte[] data = NeoDynamicStore.writeToByteArray( record );
            schemaStore.getRecordStore().writeRecord( record.getId(), data );
        }
        return first( records ).getId();
    }

    private Config config;
    private FlatNeoStores neoStores;
    private NeoSchemaStore schemaStore;
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private StoreFactory storeFactory;

    @Before
    public void before() throws Exception
    {
        config = new Config( stringMap() );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        storeFactory = new StoreFactory( config, idGeneratorFactory, fs.get(), DEV_NULL,
                new DefaultTxHook() );
        neoStores = storeFactory.openNeoStore( "" );
//        File file = new File( "schema-store" );
//        storeFactory.createSchemaStore( file );
//        schemaStore = storeFactory.newSchemaStore( file );
        schemaStore = neoStores.getSchemaStore();
    }

    @After
    public void after() throws Exception
    {
        if ( neoStores != null )
        {
            neoStores.close();
        }
    }
}
