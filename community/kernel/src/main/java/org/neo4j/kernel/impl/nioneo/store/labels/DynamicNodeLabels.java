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
package org.neo4j.kernel.impl.nioneo.store.labels;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.getRightArray;
import static org.neo4j.kernel.impl.nioneo.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray.filter;
import static org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray.stripNodeId;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsBody;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.nioneo.alt.NeoArrayStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoDynamicStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoLabelStore;
import org.neo4j.kernel.impl.nioneo.alt.Store;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

public class DynamicNodeLabels implements NodeLabels
{
    private final long labelField;
    private final NodeRecord node;

    public DynamicNodeLabels( long labelField, NodeRecord node )
    {
        this.labelField = labelField;
        this.node = node;
    }

    @Override
    public long[] get( Store labelStore )
    {
        if ( node.isLight() )
        {
            node.addLabelDynamicRecords( NeoLabelStore.ensureHeavy( node.getLabelField(), labelStore.getRecordStore() ) );
        }
        byte[] arrayData = NeoDynamicStore.readFullByteArray( node.getDynamicLabelRecords() );
        return (long[]) NeoArrayStore.getRightArray( arrayData );
    }

    @Override
    public long[] getIfLoaded()
    {
        if ( node.isLight() )
        {
            return null;
        }
        for ( DynamicRecord dynamic : node.getUsedDynamicLabelRecords() )
        {
            if ( dynamic.isLight() )
            {
                return null;
            }
        }
        return stripNodeId( (long[]) getRightArray( readFullByteArrayFromHeavyRecords(
                node.getUsedDynamicLabelRecords(), ARRAY ) ) );
    }

    public Collection<DynamicRecord> put( long[] labelIds, Store labelStore )
    {
        Collection<DynamicRecord> changedDynamicRecords = Collections.emptyList();

        if ( labelField != 0 )
        {
            // There are existing dynamic label records, get them
            if ( node.isLight() )
            {
                node.addLabelDynamicRecords( NeoLabelStore.ensureHeavy( node.getLabelField(), labelStore.getRecordStore() ) );
            }
            changedDynamicRecords = node.getDynamicLabelRecords();
            setNotInUse( changedDynamicRecords );
        }

        if ( !new InlineNodeLabels( labelField, node ).tryInlineInNodeRecord( labelIds, changedDynamicRecords ) )
        {
            Set<DynamicRecord> allRecords = new HashSet<>( changedDynamicRecords );
            Collection<DynamicRecord> allocatedRecords =
            NeoLabelStore.allocateRecordsForLabels( node.getId(), labelIds, changedDynamicRecords, labelStore );
            allRecords.addAll( allocatedRecords );
            node.setLabelField( dynamicPointer( allocatedRecords ) ); 
            node.addLabelDynamicRecords( allocatedRecords );
            changedDynamicRecords = allRecords;
        }

        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> add( long labelId, Store labelStore )
    {
        if ( node.isLight() )
        {
            node.addLabelDynamicRecords( NeoLabelStore.ensureHeavy( node.getLabelField(), labelStore.getRecordStore() ) );
        }
        Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        long[] existingLabelIds = (long[]) NeoArrayStore.getRightArray( NeoDynamicStore.readFullByteArray( existingRecords ) );
        long[] newLabelIds = LabelIdArray.concatAndSort( existingLabelIds, labelId );
        Collection<DynamicRecord> changedDynamicRecords =
            NeoLabelStore.allocateRecordsForLabels( node.getId(), newLabelIds, existingRecords, labelStore );
        node.setLabelField( dynamicPointer( changedDynamicRecords ) ); 
        node.addLabelDynamicRecords( changedDynamicRecords );
        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> remove( long labelId, Store labelStore )
    {
        if ( node.isLight() )
        {
            node.addLabelDynamicRecords( NeoLabelStore.ensureHeavy( node.getLabelField(), labelStore.getRecordStore() ) );
        }
        Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        long[] existingLabelIds = (long[]) NeoArrayStore.getRightArray( NeoDynamicStore.readFullByteArray( existingRecords ) );
        long[] newLabelIds = filter( existingLabelIds, labelId );
        if ( new InlineNodeLabels( labelField, node ).tryInlineInNodeRecord( newLabelIds, existingRecords ) )
        {
            setNotInUse( existingRecords );
        }
        else
        {
            Collection<DynamicRecord> newRecords =
            NeoLabelStore.allocateRecordsForLabels( node.getId(), newLabelIds, existingRecords, labelStore );
            node.setLabelField( dynamicPointer( newRecords ) );
            node.addLabelDynamicRecords( existingRecords );
            if ( !newRecords.equals( existingRecords ) )
            {   // One less dynamic record, mark that one as not in use
                for ( DynamicRecord record : existingRecords )
                {
                    if ( !newRecords.contains( record ) )
                    {
                        record.setInUse( false );
                        record.setLength( 0 ); // so that it will not be made heavy again...
                    }
                }
            }
        }
        return existingRecords;
    }

    @Override
    public void ensureHeavy( Store labelStore )
    {
        if ( !node.isLight() )
        {
            return;
        }
        node.addLabelDynamicRecords(  NeoLabelStore.ensureHeavy( getFirstDynamicRecordId(), labelStore.getRecordStore() ) );
    }

    public static long dynamicPointer( Collection<DynamicRecord> newRecords )
    {
        return 0x8000000000L | first( newRecords ).getId();
    }

    public long getFirstDynamicRecordId()
    {
        return parseLabelsBody( labelField );
    }

    private void setNotInUse( Collection<DynamicRecord> changedDynamicRecords )
    {
        for ( DynamicRecord record : changedDynamicRecords )
        {
            record.setInUse( false );
        }
    }

    @Override
    public boolean isInlined()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return format( "Dynamic(id:%d)", getFirstDynamicRecordId() );
    }
}
