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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.index.UpdateMode;
import org.neo4j.kernel.impl.api.index.IndexUpdates;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.core.IteratingPropertyReceiver;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.xa.Command.Mode;
import org.neo4j.kernel.impl.nioneo.xa.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.WriteTransaction.LabelChangeSummary;

class LazyIndexUpdates implements IndexUpdates
{
    private final FlatNeoStores neoStores;
    private final Collection<PropertyCommand> propCommands;
    private final Map<Long, NodeCommand> nodeCommands;
    private Collection<NodePropertyUpdate> updates;

    public LazyIndexUpdates( FlatNeoStores neoStores,
                             Collection<PropertyCommand> propCommands, Map<Long, NodeCommand> nodeCommands )
    {
        this.neoStores = neoStores;
        this.propCommands = propCommands;
        this.nodeCommands = nodeCommands;
    }

    @Override
    public Iterator<NodePropertyUpdate> iterator()
    {
        if ( updates == null )
        {
            updates = gatherPropertyAndLabelUpdates();
        }
        return updates.iterator();
    }

    @Override
    public Set<Long> changedNodeIds()
    {
        Set<Long> nodeIds = new HashSet<>( nodeCommands.keySet() );
        for ( PropertyCommand propCmd : propCommands )
        {
            PropertyRecord record = propCmd.getAfter();
            if ( record.isNodeSet() )
            {
                nodeIds.add( record.getNodeId() );
            }
        }
        return nodeIds;
    }

    private Collection<NodePropertyUpdate> gatherPropertyAndLabelUpdates()
    {
        Collection<NodePropertyUpdate> propertyUpdates = new HashSet<>();
        Map<Pair<Long, Integer>, NodePropertyUpdate> propertyChanges = new HashMap<>();
        gatherUpdatesFromPropertyCommands( propertyUpdates, propertyChanges );
        gatherUpdatesFromNodeCommands( propertyUpdates, propertyChanges );
        return propertyUpdates;
    }

    private void gatherUpdatesFromPropertyCommands( Collection<NodePropertyUpdate> updates,
                                                    Map<Pair<Long, Integer>, NodePropertyUpdate> propertyLookup )
    {
        byte[] data = new byte[neoStores.getNodeStore().getRecordStore().getRecordSize()];
        for ( PropertyCommand propertyCommand : propCommands )
        {
            PropertyRecord after = propertyCommand.getAfter();
            if ( !after.isNodeSet() )
            {
                continue;
            }

            long[] nodeLabelsBefore, nodeLabelsAfter;
            NodeCommand nodeChanges = nodeCommands.get( after.getNodeId() );
            if ( nodeChanges != null )
            {
                nodeLabelsBefore = parseLabelsField( nodeChanges.getBefore() ).get( neoStores.getLabelStore() );
                nodeLabelsAfter = parseLabelsField( nodeChanges.getAfter() ).get( neoStores.getLabelStore() );
            }
            else
            {
                /* If the node doesn't exist here then we've most likely encountered this scenario:
                 * - TX1: Node N exists and has property record P
                 * - rotate log
                 * - TX2: P gets changed
                 * - TX3: N gets deleted (also P, but that's irrelevant for this scenario)
                 * - N is persisted to disk for some reason
                 * - crash
                 * - recover
                 * - TX2: P has changed and updates to indexes are gathered. As part of that it tries to read
                 *        the labels of N (which does not exist a.t.m.).
                 *
                 * We can actually (if we disregard any potential inconsistencies) just assume that
                 * if this happens and we're in recovery mode that the node in question will be deleted
                 * in an upcoming transaction, so just skip this update.
                 */
                neoStores.getNodeStore().getRecordStore().getRecord( after.getNodeId(), data );
                NodeRecord nodeRecord = NeoNodeStore.getRecord( after.getNodeId(), data ); 

                nodeLabelsBefore = nodeLabelsAfter = parseLabelsField( nodeRecord ).get( neoStores.getLabelStore() );
            }

            Iterable<NodePropertyUpdate> propertyUpdates = new PropertyPhysicalToLogicalConverter( neoStores.getStringStore(), 
                    neoStores.getArrayStore() ).apply( propertyCommand.getBefore(), nodeLabelsBefore, after,
                            nodeLabelsAfter );
            for ( NodePropertyUpdate update : propertyUpdates )
            {
                updates.add( update );
                if ( update.getUpdateMode() == UpdateMode.CHANGED )
                {
                    propertyLookup.put( Pair.of( update.getNodeId(), update.getPropertyKeyId() ), update );
                }
            }
        }
    }

    private void gatherUpdatesFromNodeCommands( Collection<NodePropertyUpdate> propertyUpdates,
                                                Map<Pair<Long, Integer>, NodePropertyUpdate> propertyLookup )
    {
        for ( NodeCommand nodeCommand : nodeCommands.values() )
        {
            long nodeId = nodeCommand.getKey();
            long[] labelsBefore = parseLabelsField( nodeCommand.getBefore() ).get( neoStores.getLabelStore() );
            long[] labelsAfter = parseLabelsField( nodeCommand.getAfter() ).get( neoStores.getLabelStore() );

            if ( nodeCommand.getMode() != Mode.UPDATE )
            {
                // For created and deleted nodes rely on the updates from the perspective of properties to cover it all
                // otherwise we'll get duplicate update during recovery, or cannot load properties if deleted.
                continue;
            }

            LabelChangeSummary summary = new LabelChangeSummary( labelsBefore, labelsAfter );
            Iterator<DefinedProperty> properties = nodeFullyLoadProperties( nodeId );
            while ( properties.hasNext() )
            {
                DefinedProperty property = properties.next();
                int propertyKeyId = property.propertyKeyId();
                if ( summary.hasAddedLabels() )
                {
                    Object value = property.value();
                    propertyUpdates.add( add( nodeId, propertyKeyId, value, summary.getAddedLabels() ) );
                }
                if ( summary.hasRemovedLabels() )
                {
                    NodePropertyUpdate propertyChange = propertyLookup.get( Pair.of( nodeId, propertyKeyId ) );
                    Object value = propertyChange == null ? property.value() : propertyChange.getValueBefore();
                    propertyUpdates.add( remove( nodeId, propertyKeyId, value, summary.getRemovedLabels() ) );
                }
            }
        }
    }

    private Iterator<DefinedProperty> nodeFullyLoadProperties( long nodeId )
    {
        
        RecordStore nodeStore = neoStores.getNodeStore().getRecordStore();
        byte[] data = new byte[nodeStore.getRecordSize()];
        nodeStore.getRecord( nodeId, data );
        NodeRecord nodeRecord = NeoNodeStore.getRecord( nodeId, data ); 
        IteratingPropertyReceiver receiver = new IteratingPropertyReceiver();
        Collection<PropertyRecord> chain = 
                NeoPropertyStore.getPropertyRecordChain( neoStores.getPropertyStore().getRecordStore(), nodeRecord.getNextProp() );

        if ( chain != null )
        {
            for ( PropertyRecord propRecord : chain )
            {
                for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
                {
                    receiver.receive( propBlock.newPropertyData( neoStores ), propRecord.getId() );
                }
            }
        }
        return receiver;
    }
}