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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.CommonFactories.defaultFileSystemAbstraction;
import static org.neo4j.kernel.CommonFactories.defaultIdGeneratorFactory;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.alwaysAllowed;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;
import static org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore.LEGACY_VERSION;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;

public class StoreUpgraderInterruptionTestIT
{
    @Test
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMigration() throws IOException
    {
        String workingPath = "target/" + StoreUpgraderInterruptionTestIT.class.getSimpleName();
        File workingDirectory = new File( workingPath );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );
        StoreMigrator failingStoreMigrator = new StoreMigrator( new SilentMigrationProgressMonitor() )
        {

            @Override
            public void migrate( LegacyStore legacyStore, FlatNeoStores neoStores ) throws IOException
            {
                super.migrate( legacyStore, neoStores );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        try
        {
            newUpgrader( failingStoreMigrator, new DatabaseFiles( fileSystem ) ).attemptUpgrade( workingPath );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, LEGACY_VERSION ) );

        newUpgrader( new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles(fileSystem) )
            .attemptUpgrade( workingPath );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, NeoNeoStore.ALL_STORES_VERSION ) );
    }
    
    private StoreUpgrader newUpgrader( StoreMigrator migrator, DatabaseFiles files )
    {
        return new StoreUpgrader( defaultConfig(), alwaysAllowed(), new UpgradableDatabase(fileSystem), migrator,
                files, defaultIdGeneratorFactory(), defaultFileSystemAbstraction() );        
    }

    @Test
    public void shouldFailOnSecondAttemptIfPreviousAttemptMadeABackupToAvoidDamagingBackup() throws IOException
    {
        String workingPath = "target/" + StoreUpgraderInterruptionTestIT.class.getSimpleName();
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, new File( workingPath ) );

        DatabaseFiles failsOnBackup = new DatabaseFiles( fileSystem )
        {
            @Override
            public void moveToBackupDirectory( File workingDirectory, File backupDirectory )
            {
                fileSystem.mkdir( backupDirectory );
                throw new RuntimeException( "Failing to backup working directory" );
            }
        };

        assertTrue( allStoreFilesHaveVersion( fileSystem, new File( workingPath ), LEGACY_VERSION ) );

        try
        {
            newUpgrader( new StoreMigrator( new SilentMigrationProgressMonitor() ), failsOnBackup ).attemptUpgrade( workingPath );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "Failing to backup working directory", e.getMessage() );
        }

        try
        {
            newUpgrader( new StoreMigrator( new SilentMigrationProgressMonitor() ) , new DatabaseFiles( fileSystem ) )
                    .attemptUpgrade( workingPath );
            fail( "Should throw exception" );
        }
        catch ( Exception e )
        {
            assertTrue( e.getMessage().startsWith( "Cannot proceed with upgrade because there is an existing upgrade backup in the way at " ) );
        }
    }

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
}
