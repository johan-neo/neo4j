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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class StoreVersionTest
{
    @Test
    public void allStoresShouldHaveTheCurrentVersionIdentifier() throws IOException
    {
        File outputDir = new File( "target/var/" + StoreVersionTest.class.getSimpleName() );
        fs.get().mkdirs( outputDir );

        File storeFileName = new File( outputDir, StoreFactory.NEO_STORE_NAME );

        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.store_dir.name(), outputDir.getPath());
        config.put( "neo_store", storeFileName.getPath() );
        StoreFactory sf = new StoreFactory( new Config( config, GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), fs.get(), StringLogger.DEV_NULL,
                null );
        FlatNeoStores neoStores = sf.createNeoStore( outputDir.getPath() );

        throw new RuntimeException( "Implement this using store loader " );
        /*(Store[] stores = {
                neoStores.getNodeStore(),
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipTypeTokenStore(),
                neoStores.getPropertyStore(),
                neoStores.getPropertyKeyTokenStore()
        };

        for ( Store store : stores )
        {
            assertThat( store.getTypeAndVersionDescriptor(), containsString( CommonAbstractStore.ALL_STORES_VERSION ) );
        }
        neoStores.close();*/
    }

    @Test
    @Ignore
    public void shouldFailToCreateAStoreContainingOldVersionNumber() throws IOException
    {
        File outputDir = new File( "target/var/" + StoreVersionTest.class.getSimpleName() );
        assertTrue( outputDir.mkdirs() );

        URL legacyStoreResource = StoreMigrator.class.getResource( "legacystore/exampledb/neostore.nodestore.db" );
        File workingFile = new File( outputDir, "neostore.nodestore.db" );
        FileUtils.copyFile( new File( legacyStoreResource.getFile() ), workingFile );

        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );

        try
        {
            throw new RuntimeException( "implement this using store loader" );
//            new NodeStore( workingFile, config, new DefaultIdGeneratorFactory(),
//                    new DefaultWindowPoolFactory(), fs.get(), StringLogger.DEV_NULL, null );
//            fail( "Should have thrown exception" );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            //expected
        }
    }

    @Test
    public void neoStoreHasCorrectStoreVersionField() throws IOException
    {
        File outputDir = new File( "target/var/"
                + StoreVersionTest.class.getSimpleName()
                + "test2" );
        fs.get().mkdirs( outputDir );

        File storeFileName = new File( outputDir, StoreFactory.NEO_STORE_NAME );

        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.store_dir.name(), outputDir.getPath() );
        config.put( "neo_store", storeFileName.getPath() );
        StoreFactory sf = new StoreFactory( new Config( config, GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), fs.get(), StringLogger.DEV_NULL,
                null );
        throw new RuntimeException( "implement this using store loader" );
//       NeoStore neoStore = sf.createNeoStore( storeFileName );
//        // The first checks the instance method, the other the public one
//        assertEquals( CommonAbstractStore.ALL_STORES_VERSION,
//                NeoStore.versionLongToString( neoStore.getStoreVersion() ) );
//        assertEquals( CommonAbstractStore.ALL_STORES_VERSION,
//                NeoStore.versionLongToString( NeoStore.getStoreVersion( fs.get(), storeFileName ) ) );
    }

    @Test
    public void testProperEncodingDecodingOfVersionString()
    {
        String[] toTest = new String[]{"123", "foo", "0.9.9", "v0.A.0",
                "bar", "chris", "1234567"};
        for ( String string : toTest )
        {
            assertEquals(
                    string,
                    NeoNeoStore.versionLongToString( NeoNeoStore.versionStringToLong( string ) ) );
        }
    }
    
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
