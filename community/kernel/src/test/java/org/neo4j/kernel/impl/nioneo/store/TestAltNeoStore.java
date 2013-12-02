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

import java.io.File;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

public class TestAltNeoStore
{
    private FlatNeoStores neoStore;
    private TargetDirectory targetDirectory;
    private File path;

    @Rule public TargetDirectory.TestDirectory testDir = TargetDirectory.cleanTestDirForTest( getClass() );

    private File file( String name )
    {
        return new File( path, name);
    }

    @Before
    public void setUpNeoStore() throws Exception
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        targetDirectory = TargetDirectory.forTest( fs, getClass() );
        path = targetDirectory.directory( "dir", true );
        Config config = new Config( new HashMap<String, String>(), GraphDatabaseSettings.class );
        StoreFactory sf = new StoreFactory( config, new DefaultIdGeneratorFactory(), fs, StringLogger.DEV_NULL, null );
        sf.createNeoStore( path.getPath() ).close();
        neoStore = new FlatNeoStores( path.getPath(), config, new DefaultIdGeneratorFactory(), fs, StringLogger.SYSTEM );
    }
    
    @Test
    public void simpleTest()
    {
        neoStore.close();
    }
}
