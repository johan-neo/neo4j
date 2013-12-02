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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.Store;
import org.neo4j.kernel.impl.util.StringLogger;

public class TestStore
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
            new DefaultIdGeneratorFactory();
    public static FileSystemAbstraction FILE_SYSTEM =
            new DefaultFileSystemAbstraction();

    private File path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "teststore" );
        File file = new File( path );
        file.mkdirs();
        return file;
    }

    private File file( String name )
    {
        return new File( path() , name);
    }

    private File storeFile()
    {
        return file( "testStore.db" );
    }

    private File storeIdFile()
    {
        return file( "testStore.db.id" );
    }

    @Test
    public void testCreateStore() throws IOException
    {
        try
        {
            try
            {
                StoreHolder.createStore( null );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            StoreHolder store = StoreHolder.createStore( storeFile() );
            try
            {
                StoreHolder.createStore( storeFile() );
                fail( "Creating existing store should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private void deleteBothFiles()
    {
        File file = storeFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = storeIdFile();
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
    }

    @Test
    public void testStickyStore() throws IOException
    {
        try
        {
            StoreHolder.createStore( storeFile() ).close();
            java.nio.channels.FileChannel fileChannel = new java.io.RandomAccessFile(
                    storeFile(), "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            StoreHolder store = new StoreHolder( storeFile() );
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    @Test
    public void testClose() throws IOException
    {
        try
        {
            StoreHolder store = StoreHolder.createStore( storeFile() );
            store.close();
        }
        finally
        {
            deleteBothFiles();
        }
    }

    private static class StoreHolder
    {
        public static final String TYPE_DESCRIPTOR = "TestVersion";
        private static final int RECORD_SIZE = 1;
        
        private Store store;

        public StoreHolder( File fileName ) throws IOException
        {
            store = new Store( fileName, new Config( MapUtil.stringMap( "store_dir", "target/var/teststore" ),
                    GraphDatabaseSettings.class ),
                    IdType.NODE, ID_GENERATOR_FACTORY, FILE_SYSTEM, StringLogger.DEV_NULL,
                    TYPE_DESCRIPTOR, false, RECORD_SIZE );
        }

        public int getRecordSize()
        {
            return RECORD_SIZE;
        }

        public String getTypeDescriptor()
        {
            return TYPE_DESCRIPTOR;
        }

        public static StoreHolder createStore( File fileName ) throws IOException
        {
            new StoreFactory( new Config( Collections.<String, String>emptyMap(), GraphDatabaseSettings.class ),
                    ID_GENERATOR_FACTORY,
                    FILE_SYSTEM, StringLogger.DEV_NULL, null ).
                    createEmptyStore( fileName, NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ) );
            return new StoreHolder( fileName );
        }

        public void close()
        {
            store.close();
        }
    }
}