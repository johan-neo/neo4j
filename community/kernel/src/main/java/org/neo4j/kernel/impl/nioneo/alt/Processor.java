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

import java.util.Iterator;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

public abstract class Processor<FAILURE extends Exception>
{
    // Have it volatile so that it can be stopped from a different thread.
    private volatile boolean continueScanning = true;

    public void stopScanning()
    {
        continueScanning = false;
    }

    public void processSchema( RecordStore schemaStore, DynamicRecord schema ) throws FAILURE
    {
        processRecord( DynamicRecord.class, schemaStore, schema );
    }

    public void processNode( RecordStore nodeStore, NodeRecord node ) throws FAILURE
    {
        processRecord( NodeRecord.class, nodeStore, node );
    }

    public void processRelationship( RecordStore relationshipStore, RelationshipRecord rel ) throws FAILURE
    {
        processRecord( RelationshipRecord.class, relationshipStore, rel );
    }

    public void processProperty( RecordStore propertyStore, PropertyRecord property ) throws FAILURE
    {
        processRecord( PropertyRecord.class, propertyStore, property );
    }

    public void processString( RecordStore stringStore, DynamicRecord string, IdType idType ) throws FAILURE
    {
        processDynamic( stringStore, string );
    }

    public void processArray( RecordStore arrayStore, DynamicRecord array ) throws FAILURE
    {
        processDynamic( arrayStore, array );
    }

    public void processLabelArrayWithOwner( RecordStore labelStore, DynamicRecord labelArray )
            throws FAILURE
    {
        processDynamic( labelStore, labelArray );
    }

    protected void processDynamic( RecordStore dynamicStore, DynamicRecord record ) throws FAILURE
    {
        processRecord( DynamicRecord.class, dynamicStore, record );
    }

    public void processRelationshipTypeToken( RecordStore relationshipTypeStore,
                                              RelationshipTypeTokenRecord record ) throws FAILURE
    {
        processRecord( RelationshipTypeTokenRecord.class, relationshipTypeStore, record );
    }

    public void processPropertyKeyToken( RecordStore propertyKeyTokenStore, PropertyKeyTokenRecord record ) throws FAILURE
    {
        processRecord( PropertyKeyTokenRecord.class, propertyKeyTokenStore, record );
    }

    public void processLabelToken( RecordStore labelTokenStore, LabelTokenRecord record ) throws FAILURE
    {
        processRecord( LabelTokenRecord.class, labelTokenStore, record);
    }

    @SuppressWarnings("UnusedParameters")
    protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, RecordStore store, R record ) throws FAILURE
    {
        throw new UnsupportedOperationException( this + " does not process "
                                                 + type.getSimpleName().replace( "Record", "" ) + " records" );
    }

    public <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore store,
            final Predicate<? super R>... filters )
    {
        return new Iterable<R>()
        {
            @Override
            public Iterator<R> iterator()
            {
                return new PrefetchingIterator<R>()
                {
                    final long highId = store.getHighestPossibleIdInUse();
                    int id = 0;

                    @Override
                    protected R fetchNextOrNull()
                    {
                        scan: while ( id <= highId && id >= 0 )
                        {
                            if (!continueScanning)
                            {
                                return null;
                            }
                            R record = getRecord( store, id++ );
                            for ( Predicate<? super R> filter : filters )
                            {
                                if ( !filter.accept( record ) ) continue scan;
                            }
                            return record;
                        }
                        return null;
                    }
                };
            }
        };
    }

    protected <R extends AbstractBaseRecord> R getRecord( RecordStore store, long id )
    {
        throw new RuntimeException( "Implement this" );
        // return store.forceGetRecord( id );
    }

    public static <R extends AbstractBaseRecord> Iterable<R> scanById( final RecordStore store,
            Iterable<Long> ids )
    {
        return new IterableWrapper<R, Long>( ids )
        {
            @Override
            protected R underlyingObjectToObject( Long id )
            {
                throw new RuntimeException( "Implement this" );     
                // return store.forceGetRecord( id );
            }
        };
    }

    public <R extends AbstractBaseRecord> void applyById( RecordStore store, Iterable<Long> ids ) throws FAILURE
    {
        throw new RuntimeException( "Implement this" );
//        for ( R record : scanById( store, ids ) )
//            store.accept( this, record );
    }

    public <R extends AbstractBaseRecord> void applyFiltered( RecordStore store, Predicate<? super R>... filters ) throws FAILURE
    {
        apply( store, ProgressListener.NONE, filters );
    }

    public <R extends AbstractBaseRecord> void applyFiltered( RecordStore store, ProgressListener progressListener,
            Predicate<? super R>... filters ) throws FAILURE
    {
        apply( store, progressListener, filters );
    }

    private <R extends AbstractBaseRecord> void apply( RecordStore store, ProgressListener progressListener,
            Predicate<? super R>... filters ) throws FAILURE
    {
        for ( R record : scan( store, filters ) )
        {
            throw new RuntimeException( "Implement this" );
//            store.accept( this, record );
//            progressListener.set( record.getLongId() );
        }
        progressListener.done();
    }

}
