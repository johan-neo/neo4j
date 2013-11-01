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
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public interface OldRecordStore<R extends AbstractBaseRecord> extends IdSequence 
{
    File getStorageFileName();

    WindowPoolStats getWindowPoolStats();

    long getHighId();

    long getHighestPossibleIdInUse();

    R getRecord( long id );

    Long getNextRecordReference( R record );

    Collection<R> getRecords( long id );

    void updateRecord( R record );

    R forceGetRecord( long id );

    R forceGetRaw( R record );

    R forceGetRaw( long id );

    void forceUpdateRecord( R record );

    <FAILURE extends Exception> void accept( Processor<FAILURE> processor, R record ) throws FAILURE;

    int getRecordSize();

    int getRecordHeaderSize();

    void close();
    
    Predicate<AbstractBaseRecord> IN_USE = new Predicate<AbstractBaseRecord>()
    {
        @Override
        public boolean accept( AbstractBaseRecord item )
        {
            return item.inUse();
        }
    };

    @SuppressWarnings("unchecked")
    abstract class Processor<FAILURE extends Exception>
    {
        // Have it volatile so that it can be stopped from a different thread.
        private volatile boolean continueScanning = true;

        public void stopScanning()
        {
            continueScanning = false;
        }

        public void processSchema( OldRecordStore<DynamicRecord> store, DynamicRecord schema ) throws FAILURE
        {
            processRecord( DynamicRecord.class, store, schema );
        }

        public void processNode( OldRecordStore<NodeRecord> store, NodeRecord node ) throws FAILURE
        {
            processRecord( NodeRecord.class, store, node );
        }

        public void processRelationship( OldRecordStore<RelationshipRecord> store, RelationshipRecord rel ) throws FAILURE
        {
            processRecord( RelationshipRecord.class, store, rel );
        }

        public void processProperty( OldRecordStore<PropertyRecord> store, PropertyRecord property ) throws FAILURE
        {
            processRecord( PropertyRecord.class, store, property );
        }

        public void processString( OldRecordStore<DynamicRecord> store, DynamicRecord string, 
                                    @SuppressWarnings( "deprecation") IdType idType ) throws FAILURE
        {
            processDynamic( store, string );
        }

        public void processArray( OldRecordStore<DynamicRecord> store, DynamicRecord array ) throws FAILURE
        {
            processDynamic( store, array );
        }

        public void processLabelArrayWithOwner( OldRecordStore<DynamicRecord> store, DynamicRecord labelArray )
                throws FAILURE
        {
            processDynamic( store, labelArray );
        }

        protected void processDynamic( OldRecordStore<DynamicRecord> store, DynamicRecord record ) throws FAILURE
        {
            processRecord( DynamicRecord.class, store, record );
        }

        public void processRelationshipTypeToken( OldRecordStore<RelationshipTypeTokenRecord> store,
                                                  RelationshipTypeTokenRecord record ) throws FAILURE
        {
            processRecord( RelationshipTypeTokenRecord.class, store, record );
        }

        public void processPropertyKeyToken( OldRecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record ) throws FAILURE
        {
            processRecord( PropertyKeyTokenRecord.class, store, record );
        }

        public void processLabelToken( OldRecordStore<LabelTokenRecord> store, LabelTokenRecord record ) throws FAILURE
        {
            processRecord(LabelTokenRecord.class, store, record);
        }

        @SuppressWarnings("UnusedParameters")
        protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, OldRecordStore<R> store, R record ) throws FAILURE
        {
            throw new UnsupportedOperationException( this + " does not process "
                                                     + type.getSimpleName().replace( "Record", "" ) + " records" );
        }

        @SafeVarargs
        public <R extends AbstractBaseRecord> Iterable<R> scan( final OldRecordStore<R> store,
                final Predicate<? super R>... filters )
        {
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store );

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan: while ( ids.hasNext() && continueScanning )
                            {
                                R record = getRecord( store, ids.next() );
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

        protected <R extends AbstractBaseRecord> R getRecord( OldRecordStore<R> store, long id )
        {
            return store.forceGetRecord( id );
        }

        public static <R extends AbstractBaseRecord> Iterable<R> scanById( final OldRecordStore<R> store,
                Iterable<Long> ids )
        {
            return new IterableWrapper<R, Long>( ids )
            {
                @Override
                protected R underlyingObjectToObject( Long id )
                {
                    return store.forceGetRecord( id );
                }
            };
        }

        public <R extends AbstractBaseRecord> void applyById( OldRecordStore<R> store, Iterable<Long> ids ) throws FAILURE
        {
            for ( R record : scanById( store, ids ) )
                store.accept( this, record );
        }

        public <R extends AbstractBaseRecord> void applyFiltered( OldRecordStore<R> store, Predicate<? super R>... filters ) throws FAILURE
        {
            apply( store, ProgressListener.NONE, filters );
        }

        public <R extends AbstractBaseRecord> void applyFiltered( OldRecordStore<R> store, ProgressListener progressListener,
                Predicate<? super R>... filters ) throws FAILURE
        {
            apply( store, progressListener, filters );
        }

        private <R extends AbstractBaseRecord> void apply( OldRecordStore<R> store, ProgressListener progressListener,
                Predicate<? super R>... filters ) throws FAILURE
        {
            for ( R record : scan( store, filters ) )
            {
                store.accept( this, record );
                progressListener.set( record.getLongId() );
            }
            progressListener.done();
        }

    }
}
