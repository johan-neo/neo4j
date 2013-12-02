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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.neo4j.kernel.api.index.NodePropertyUpdate.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.Processor;
import org.neo4j.kernel.impl.nioneo.alt.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;

import org.neo4j.kernel.impl.nioneo.store.StoreIdIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.kernel.api.index.NodePropertyUpdate.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;

public class NeoStoreIndexStoreView implements IndexStoreView
{
    private final LockService locks;
    private final FlatNeoStores neoStores;


    public NeoStoreIndexStoreView( LockService locks, FlatNeoStores neoStores )
    {
        this.locks = locks;
        this.neoStores = neoStores;
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodesWithPropertyAndLabel(
            IndexDescriptor descriptor, final Visitor<NodePropertyUpdate, FAILURE> visitor )
    {
<       final int soughtLabelId = descriptor.getLabelId();
        final int soughtPropertyKeyId = descriptor.getPropertyKeyId();
        return new NodeStoreScan<NodePropertyUpdate, FAILURE>()
        {
            @Override
            protected NodePropertyUpdate read( NodeRecord node )
            {
                long[] labels = parseLabelsField( node ).get( nodeStore );
                if ( !containsLabel( soughtLabelId, labels ) )
                {
                    return null;
                }
                for ( PropertyBlock property : properties( node ) )
                {
                    int propertyKeyId = property.getKeyIndexId();
                    if ( soughtPropertyKeyId == propertyKeyId )
                    {
                        return NodePropertyUpdate.add( node.getId(), propertyKeyId, valueOf( property ), labels );
                    }
                }
                return null;
               /* // Create a processor that for each accepted node (containing the desired label) looks through its properties,
                // getting the desired one (if any) and feeds to the index manipulator.
                LabelsReference labelsReference = new LabelsReference();
                OldRecordStore.Processor<FAILURE> processor = new NodePropertyUpdateProcessor<>( singleIntPredicate( descriptor.getPropertyKeyId() ),
                        labelsReference, visitor );

                // Run the processor for the nodes containing the given label.
                // TODO When we've got a decent way of getting nodes with a label, use that instead.
                Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( singleIntPredicate( descriptor.getLabelId() ), labelsReference );
            */
            }

            @Override
            protected void process( NodePropertyUpdate update ) throws FAILURE
            {
                visitor.visit( update );
            }
        };
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            final int[] labelIds, final int[] propertyKeyIds,
            final Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor,
            final Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor )
    {
        return new NodeStoreScan<Update, FAILURE>()
        {
            @Override
            protected Update read( NodeRecord node )
            {
                long[] labels = parseLabelsField( node ).get( nodeStore );
                Update update = new Update( node.getId(), labels );
                if ( !containsAnyLabel( labelIds, labels ) )
                {
                    return update;
                }
                properties: for ( PropertyBlock property : properties( node ) )
                {
                    int propertyKeyId = property.getKeyIndexId();
                    for ( int sought : propertyKeyIds )
                    {
                        if ( propertyKeyId == sought )
                        {
                            update.add( NodePropertyUpdate
                                                .add( node.getId(), propertyKeyId, valueOf( property ), labels ) );
                            continue properties;
                        }
                    }
                }
                return update;
   /*             // Create a processor that for each accepted node (containing the desired label) looks through its properties,
                // getting the desired one (if any) and feeds to the index manipulator.
                LabelsReference labelsReference = new LabelsReference();
                NodePropertyUpdateProcessor<FAILURE> propertyUpdateProcessor = new NodePropertyUpdateProcessor<>( multipleIntPredicate( propertyKeyIds ),
                        labelsReference, propertyUpdateVisitor );
                Predicate<NodeRecord> predicate = new NodeLabelFilterPredicate( multipleIntPredicate( labelIds ), labelsReference );

                // Wrap the property processor inside another processor that processes everything, produces
                // label updates and delegates to the property processor.
                OldRecordStore.Processor<FAILURE> processor =
                        new NodeProcessor<>( propertyUpdateProcessor, predicate, labelsReference, labelUpdateVisitor );
     */
            }

            @Override
            protected void process( Update update ) throws FAILURE
            {
                labelUpdateVisitor.visit( update.labels );
                for ( NodePropertyUpdate propertyUpdate : update )
                {
                    propertyUpdateVisitor.visit( propertyUpdate );
                }
            }
        };
    }

    @Override
    public Iterable<NodePropertyUpdate> nodeAsUpdates( long nodeId )
    {
        NodeRecord node = NeoNodeStore.getRecord( nodeId, 
                neoStores.getNodeStore().getRecordStore().getRecord( nodeId ), RecordLoad.FORCE ); 

        if ( !node.inUse() )
        {
            return Iterables.empty(); // node not in use => no updates
        }
        long firstPropertyId = node.getCommittedNextProp();
        if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
        {
            return Iterables.empty(); // no properties => no updates (it's not going to be in any index)
        }
        long[] labels = parseLabelsField( node ).get( neoStores.getLabelStore() );
        if ( labels.length == 0 )
        {
            return Iterables.empty(); // no labels => no updates (it's not going to be in any index)
        }
        ArrayList<NodePropertyUpdate> updates = new ArrayList<>();
        for ( PropertyRecord propertyRecord : NeoPropertyStore.getPropertyRecordChain( neoStores.getPropertyStore().getRecordStore(), firstPropertyId ) )
        {
            for ( PropertyBlock property : propertyRecord.getPropertyBlocks() )
            {
                Object value = null;
                if ( property.getType() == PropertyType.STRING )
                {
                    value = property.getType().getValue( property, neoStores.getStringStore().getRecordStore() );
                }
                else if ( property.getType() == PropertyType.ARRAY )
                {
                    value = property.getType().getValue( property, neoStores.getArrayStore().getRecordStore() );
                }
                else
                {
                    value = property.getType().getValue( property, null );
                }
 
                updates.add( NodePropertyUpdate.add( node.getId(), property.getKeyIndexId(), value, labels ) );
            }
        }
        return updates;
    }

    private Object valueOf( PropertyBlock property )
    {
        // Make sure the value is loaded, even if it's of a "heavy" kind.
        propertyStore.ensureHeavy( property );
        return property.getType().getValue( property, propertyStore );
    }

    private Iterable<PropertyBlock> properties( final NodeRecord node )
    {
        return new Iterable<PropertyBlock>()
        {
            @Override
            public Iterator<PropertyBlock> iterator()
            {
                return new PropertyBlockIterator( node );
            }
        };
    }

    private static boolean containsLabel( int sought, long[] labels )
    {
        for ( long label : labels )
        {
            if ( label == sought )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAnyLabel( int[] soughtIds, long[] labels )
    {
        for ( int soughtId : soughtIds )
        {
            if ( containsLabel( soughtId, labels ) )
            {
                return true;
            }
        }
        return false;
    }

    private static class Update implements Iterable<NodePropertyUpdate>
    {
        private final NodeLabelUpdate labels;
        private final List<NodePropertyUpdate> propertyUpdates = new ArrayList<>();

        Update( long nodeId, long[] labels )
        {
            this.labels = labelChanges( nodeId, EMPTY_LONG_ARRAY, labels );
        }

        void add( NodePropertyUpdate update )
        {
            propertyUpdates.add( update );
        }

        @Override
        public Iterator<NodePropertyUpdate> iterator()
        {
            return propertyUpdates.iterator();
        }
    }

    private class PropertyBlockIterator extends PrefetchingIterator<PropertyBlock>
    {
        private final Iterator<PropertyRecord> records;
        private Iterator<PropertyBlock> blocks = IteratorUtil.emptyIterator();

        PropertyBlockIterator( NodeRecord node )
        {
            long firstPropertyId = node.getCommittedNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                records = IteratorUtil.emptyIterator();
            }
            else
            {
                records = propertyStore.getPropertyRecordChain( firstPropertyId ).iterator();
            }
        }

        @Override
        protected PropertyBlock fetchNextOrNull()
        {
            for (; ; )
            {
                if ( blocks.hasNext() )
                {
                    return blocks.next();
                }
                if ( !records.hasNext() )
                {
                    return null;
                }
                blocks = records.next().getPropertyBlocks().iterator();
            }
        }
    }

    private abstract class NodeStoreScan<RESULT, FAILURE extends Exception> implements StoreScan<FAILURE>
    {
        private volatile boolean continueScanning;

        protected abstract RESULT read( NodeRecord node );

        protected abstract void process( RESULT result ) throws FAILURE;

        @Override
        public void run() throws FAILURE
        {
            PrimitiveLongIterator nodeIds = new StoreIdIterator( nodeStore );
            continueScanning = true;
            while ( continueScanning && nodeIds.hasNext() )
            {
                long id = nodeIds.next();
                RESULT result = null;
                try ( Lock ignored = locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) )
                {
                    NodeRecord record = nodeStore.forceGetRecord( id );
                    if ( record.inUse() )
                    {
                        result = read( record );
                    }
                }
                if ( result != null )
                {
                    process( result );
                }
            }
        }

        @Override
        public void stop()
        {
            continueScanning = false;
        }
    }
}
