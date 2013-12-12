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

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoDynamicStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStringStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoTokenNameStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoTokenStore;
import org.neo4j.kernel.impl.nioneo.alt.NewDynamicRecordAllocator;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

/**
 * Migrates a neo4j database from one version to the next. Instantiated with a {@link LegacyStore}
 * representing the old version and a {@link NeoStore} representing the new version.
 * 
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 */
public class StoreMigrator
{
    private final MigrationProgressMonitor progressMonitor;

    public StoreMigrator( MigrationProgressMonitor progressMonitor )
    {
        this.progressMonitor = progressMonitor;
    }

    public void migrate( LegacyStore legacyStore, FlatNeoStores neoStores ) throws IOException
    {
        progressMonitor.started();
        new Migration( legacyStore, neoStores ).migrate();
        progressMonitor.finished();
    }
    
    protected class Migration
    {
        private final LegacyStore legacyStore;
        private final FlatNeoStores neoStores;
        private final long totalEntities;
        private int percentComplete;

        public Migration( LegacyStore legacyStore, FlatNeoStores neoStores )
        {
            this.legacyStore = legacyStore;
            this.neoStores = neoStores;
            totalEntities = legacyStore.getNodeStoreReader().getMaxId();
        }

        private void migrate() throws IOException
        {
            // Migrate
            migrateNeoStore( neoStores );
            migrateNodes( neoStores.getNodeStore() ); 
            migratePropertyIndexes( neoStores );

            // Close
            neoStores.close();
            legacyStore.close();

            // Just copy unchanged stores that doesn't need migration
            legacyStore.copyRelationshipStore( neoStores );
            legacyStore.copyRelationshipTypeTokenStore( neoStores );
            legacyStore.copyRelationshipTypeTokenNameStore( neoStores );
            legacyStore.copyDynamicStringPropertyStore( neoStores );
            legacyStore.copyDynamicArrayPropertyStore( neoStores );
        }

        private void migratePropertyIndexes( FlatNeoStores neoStores ) throws IOException
        {
            Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();
            
            // dedup and write new property key token store (incl. names)
            Map<Integer, Integer> propertyKeyTranslation =
                    dedupAndWritePropertyKeyTokenStore( neoStores.getPropertyKeyTokenStore(), neoStores.getPropertyKeyTokenNameStore(), tokens );
            
            // read property store, replace property key ids
            migratePropertyStore( propertyKeyTranslation, neoStores.getPropertyStore() );
        }

        private void migrateNeoStore( FlatNeoStores neoStores ) throws IOException
        {
            legacyStore.copyNeoStore( neoStores );
            byte[] data = new byte[NeoNeoStore.RECORD_SIZE];
            NeoNeoStore.updateLong( data, NeoNeoStore.versionStringToLong( NeoNeoStore.ALL_STORES_VERSION ) );
            neoStores.getNeoStore().getRecordStore().writeRecord( NeoNeoStore.STORE_VERSION_POSITION, data );
        }

        private Map<Integer, Integer> dedupAndWritePropertyKeyTokenStore( NeoTokenStore propertyKeyTokenStore, NeoTokenNameStore propertyKeyTokenNameStore, 
                Token[] tokens /*ordered ASC*/ )
        {
            Map<Integer/*duplicate*/, Integer/*use this instead*/> translations = new HashMap<Integer, Integer>();
            Map<String, Integer> createdTokens = new HashMap<String, Integer>();
            for ( Token token : tokens )
            {
                Integer id = createdTokens.get( token.name() );
                if ( id == null )
                {   // Not a duplicate, add to store
                    id = (int) propertyKeyTokenStore.getIdGenerator().nextId();
                    PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
                    Collection<DynamicRecord> nameRecords =
                            NeoDynamicStore.allocateRecordsFromBytes( NeoPropertyStringStore.encodeString( token.name() ), 
                                    Collections.<DynamicRecord>emptyList().iterator(), new NewDynamicRecordAllocator( propertyKeyTokenNameStore, DynamicRecord.Type.UNKNOWN ) ); 
                    record.setNameId( (int) first( nameRecords ).getId() );
                    record.addNameRecords( nameRecords );
                    record.setInUse( true );
                    record.setCreated();
                    NeoTokenStore.updateToken( propertyKeyTokenStore.getRecordStore(), propertyKeyTokenNameStore.getRecordStore(), 
                            record, new byte[NeoTokenStore.LABEL_TOKEN_RECORD_SIZE] );
                    createdTokens.put( token.name(), id );
                }
                translations.put( token.id(), id );
            }
            return translations;
        }
        
        private void migratePropertyStore( Map<Integer, Integer> propertyKeyTranslation,
                NeoPropertyStore propertyStore ) throws IOException
        {
            long lastInUseId = -1;
            for ( PropertyRecord propertyRecord : loop( legacyStore.getPropertyStoreReader().readPropertyStore() ) )
            {
                // Translate property keys
                for ( PropertyBlock block : propertyRecord.getPropertyBlocks() )
                {
                    int key = block.getKeyIndexId();
                    Integer translation = propertyKeyTranslation.get( key );
                    if ( translation != null )
                    {
                        block.setKeyIndexId( translation );
                    }
                }
                propertyStore.getIdGenerator().setHighId( propertyRecord.getId()+1 );
                propertyStore.getRecordStore().writeRecord(propertyRecord.getId(), 
                        NeoPropertyStore.updateRecord( propertyRecord, new byte[NeoPropertyStore.RECORD_SIZE] ) );
                for ( long id = lastInUseId+1; id < propertyRecord.getId(); id++ )
                {
                    propertyStore.getIdGenerator().freeId( id );
                }
                lastInUseId = propertyRecord.getId();
            }
        }
        
        private void migrateNodes( NeoNodeStore nodeStore ) throws IOException
        {
            for ( NodeRecord nodeRecord : loop( legacyStore.getNodeStoreReader().readNodeStore() ) )
            {
                reportProgress( nodeRecord.getId() );
                nodeStore.getIdGenerator().setHighId( nodeRecord.getId() + 1 );
                if ( nodeRecord.inUse() )
                {
                    byte[] data = new byte[NeoNodeStore.RECORD_SIZE];
                    NeoNodeStore.updateRecord( nodeRecord, data, false );
                    nodeStore.getRecordStore().writeRecord( nodeRecord.getId(), data );
                }
                else
                {
                    nodeStore.getIdGenerator().freeId( nodeRecord.getId() );
                }
            }
            legacyStore.getNodeStoreReader().close();
        }

        private void reportProgress( long id )
        {
            int newPercent = (int) (id * 100 / totalEntities);
            if ( newPercent > percentComplete )
            {
                percentComplete = newPercent;
                progressMonitor.percentComplete( percentComplete );
            }
        }
    }
}
