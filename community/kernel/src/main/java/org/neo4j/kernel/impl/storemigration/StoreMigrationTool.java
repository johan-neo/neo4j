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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 * 
 * @see StoreMigrator
 */
public class StoreMigrationTool
{
    public static void main( String[] args ) throws IOException
    {
        String legacyStoreDirectory = args[0];
        String targetStoreDirectory = args[1];

        new StoreMigrationTool().run( legacyStoreDirectory, targetStoreDirectory, StringLogger.SYSTEM );
    }

    private void run( String legacyStoreDirectory, String targetStoreDirectory, StringLogger log ) throws IOException
    {
        LegacyStore legacyStore = new LegacyStore( new DefaultFileSystemAbstraction(),
                new File( new File( legacyStoreDirectory ), StoreFactory.NEO_STORE_NAME ) );

        Map<String, String> config = new HashMap<String, String>();

        File targetStoreDirectoryFile = new File( targetStoreDirectory );
        if ( targetStoreDirectoryFile.exists() )
        {
            throw new IllegalStateException( "Cannot migrate to a directory that already exists, " +
                    "please delete first and re-run" );
        }
        boolean success = targetStoreDirectoryFile.mkdirs();
        if ( !success )
        {
            throw new IllegalStateException( "Failed to create directory" );
        }

        config.put( GraphDatabaseSettings.store_dir.name(), targetStoreDirectory );
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

        FlatNeoStores neoStores = new StoreFactory( new Config( config, GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), fileSystem, log, null ).createNeoStore( targetStoreDirectory );

        long startTime = System.currentTimeMillis();

        new StoreMigrator( new VisibleMigrationProgressMonitor( log, System.out ) ).migrate( legacyStore, neoStores );

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf( "Migration completed in %d s%n", duration / 1000 );

        neoStores.close();

        GraphDatabaseService database =
                new GraphDatabaseFactory().newEmbeddedDatabase( targetStoreDirectoryFile.getPath() );
        database.shutdown();
    }
}
