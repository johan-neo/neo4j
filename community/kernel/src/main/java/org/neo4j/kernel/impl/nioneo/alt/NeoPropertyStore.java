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
package org.neo4j.kernel.impl.nioneo.alt;

import static org.neo4j.helpers.collection.IteratorUtil.first;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.LongerShortString;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.ShortArray;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

/**
 * Implementation of the property store. This implementation has two dynamic
 * stores. One used to store keys and another for string property values.
 */
public class NeoPropertyStore extends Store
{
    
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    public static final String TYPE_DESCRIPTOR = "PropertyStore";

    public static final int RECORD_SIZE = 1/*next and prev high bits*/
    + 4/*next*/
    + 4/*prev*/
    + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
    // = 41

    public static final IdType ID_TYPE = IdType.PROPERTY;
    
    public NeoPropertyStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.PROPERTY_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, false, RECORD_SIZE );
    }

    public static byte[] updateRecord( PropertyRecord record, byte[] data )
    {
        // registerIdFromUpdateRecord( id );
        ByteBuffer buffer = ByteBuffer.wrap( data ); 
        if ( record.inUse() )
        {
            // Set up the record header
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ( ( record.getPrevProp() & 0xF00000000L ) >> 28 );
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ( ( record.getNextProp() & 0xF00000000L ) >> 32 );
            byte modifiers = (byte) ( prevModifier | nextModifier );
            /*
             * [pppp,nnnn] previous, next high bits
             */
            buffer.put( modifiers );
            buffer.putInt( (int) record.getPrevProp() ).putInt(
                    (int) record.getNextProp() );

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            for ( PropertyBlock block : record.getPropertyBlocks() )
            {
                long[] propBlockValues = block.getValueBlocks();
                for ( long propBlockValue : propBlockValues )
                {
                    buffer.putLong( propBlockValue );
                }

                longsAppended += propBlockValues.length;
                
                
                /*
                 * For each block we need to update its dynamic record chain if
                 * it is just created. Deleted dynamic records are in the property
                 * record and dynamic records are never modified. Also, they are
                 * assigned as a whole, so just checking the first should be enough.
                 */
                // move this elsewhere
//                if ( !block.isLight()
//                     && block.getValueRecords().get( 0 ).isCreated() )
//                {
//                    updateDynamicRecords2( block.getValueRecords() );
//                }
                
            }
            if ( longsAppended < PropertyType.getPayloadSizeLongs() )
            {
                buffer.putLong( 0 );
            }
        }
        else
        {
            // skip over the record header, nothing useful there
            buffer.position( 9 ); 
            buffer.putLong( 0 );
        }
        return data;
        // move this elsewhere
        // updateDynamicRecords( record.getDeletedRecords() );
    }

    private static void updateDynamicRecords2( List<DynamicRecord> valueRecords )
    {
        throw new RuntimeException( "Implement this elsewhere" );
    }

    static private void updateDynamicRecords( List<Pair<PropertyType, DynamicRecord>> list )
    {
        throw new RuntimeException( "Implement this elsewhere" );
    }

    static private PropertyRecord getRecordFromBuffer( long id, byte[] data )
    {
        // int offsetAtBeggining = buffer.getOffset();
        ByteBuffer buffer = ByteBuffer.wrap( data );
        PropertyRecord record = new PropertyRecord( id );

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = buffer.get();
        long prevMod = ( modifiers & 0xF0L ) << 28;
        long nextMod = ( modifiers & 0x0FL ) << 32;
        long prevProp = NeoNeoStore.unsginedInt( buffer.getInt() );
        long nextProp = NeoNeoStore.unsginedInt( buffer.getInt() );
        record.setPrevProp( NeoNeoStore.longFromIntAndMod( prevProp, prevMod ) );
        record.setNextProp( NeoNeoStore.longFromIntAndMod( nextProp, nextMod ) );

        // while ( buffer.getOffset() - offsetAtBeggining < RECORD_SIZE )
        while ( buffer.position() < RECORD_SIZE )
        {
            PropertyBlock newBlock = getPropertyBlock( buffer );
            if ( newBlock != null )
            {
                record.addPropertyBlock( newBlock );
                record.setInUse( true );
            }
            else
            {
                // We assume that storage is defragged
                break;
            }
        }
        return record;
    }

    public static PropertyRecord getRecord( long id, byte[] data )
    {
        return getRecord( id, data, RecordLoad.NORMAL );
    }
    
    public static PropertyRecord getRecord( long id, byte[] data, RecordLoad load )
    {
        PropertyRecord toReturn = getRecordFromBuffer( id, data );
        if ( !toReturn.inUse() && load != RecordLoad.FORCE )
        {
            throw new InvalidRecordException( "PropertyRecord[" + id + "] not in use" );
        }
        return toReturn;
    }

    /*
     * It is assumed that the argument does hold a property block - all zeros is
     * a valid (not in use) block, so even if the Bits object has been exhausted a
     * result is returned, that has inUse() return false. Also, the argument is not
     * touched.
     */
    static private PropertyBlock getPropertyBlock( ByteBuffer buffer )
    {
        long header = buffer.getLong();
        PropertyType type = PropertyType.getPropertyType( header, true );
        if ( type == null )
        {
            return null;
        }
        PropertyBlock toReturn = new PropertyBlock();
        // toReturn.setInUse( true );
        int numBlocks = type.calculateNumberOfBlocksUsed( header );
        long[] blockData = new long[numBlocks];
        blockData[0] = header; // we already have that
        for ( int i = 1; i < numBlocks; i++ )
        {
            blockData[i] = buffer.getLong();
        }
        toReturn.setValueBlocks( blockData );
        return toReturn;
    }


    static public void encodeValue( PropertyBlock block, int keyId, Object value, FlatNeoStores neoStores )
    {
        if ( value instanceof String )
        {   // Try short string first, i.e. inlined in the property block
            String string = (String) value;
            if ( LongerShortString.encode( keyId, string, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }

            // Fall back to dynamic string store
            byte[] encodedString = NeoPropertyStringStore.encodeString( string );
            Collection<DynamicRecord> valueRecords = NeoDynamicStore.allocateRecordsFromBytes( encodedString, neoStores.getStringStore(), DynamicRecord.Type.STRING ); 
            setSingleBlockValue( block, keyId, PropertyType.STRING, first( valueRecords ).getId() );
            for ( DynamicRecord valueRecord : valueRecords )
            {
//                valueRecord.setType( PropertyType.STRING.intValue() );
                block.addValueRecord( valueRecord );
            }
        }
        else if ( value instanceof Integer )
        {
            setSingleBlockValue( block, keyId, PropertyType.INT, ((Integer) value).longValue() );
        }
        else if ( value instanceof Boolean )
        {
            setSingleBlockValue( block, keyId, PropertyType.BOOL, ((Boolean) value ? 1L : 0L) );
        }
        else if ( value instanceof Float )
        {
            setSingleBlockValue( block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits( (Float) value ) );
        }
        else if ( value instanceof Long )
        {
            long keyAndType = keyId | (((long) PropertyType.LONG.intValue()) << 24);
            if ( ShortArray.LONG.getRequiredBits( (Long) value ) <= 35 )
            {   // We only need one block for this value, special layout compared to, say, an integer
                block.setSingleBlock( keyAndType | (1L << 28) | ((Long) value << 29) );
            }
            else
            {   // We need two blocks for this value
                block.setValueBlocks( new long[]{keyAndType, (Long) value} );
            }
        }
        else if ( value instanceof Double )
        {
            block.setValueBlocks( new long[]{
                    keyId | (((long) PropertyType.DOUBLE.intValue()) << 24),
                    Double.doubleToRawLongBits( (Double) value )} );
        }
        else if ( value instanceof Byte )
        {
            setSingleBlockValue( block, keyId, PropertyType.BYTE, ((Byte) value).longValue() );
        }
        else if ( value instanceof Character )
        {
            setSingleBlockValue( block, keyId, PropertyType.CHAR, (Character) value );
        }
        else if ( value instanceof Short )
        {
            setSingleBlockValue( block, keyId, PropertyType.SHORT, ((Short) value).longValue() );
        }
        else if ( value.getClass().isArray() )
        {   // Try short array first, i.e. inlined in the property block
            if ( ShortArray.encode( keyId, value, block, PropertyType.getPayloadSize() ) )
            {
                return;
            }
            NewDynamicRecordAllocator allocator = new NewDynamicRecordAllocator( neoStores.getArrayStore(), DynamicRecord.Type.ARRAY );
            Collection<DynamicRecord> arrayRecords = null;
            Class<?> type = value.getClass().getComponentType();
            if ( type.equals( String.class ) )
            {
                arrayRecords = NeoPropertyArrayStore.allocateFromString( (String[]) value, Collections.<DynamicRecord>emptyList(), allocator );
            }
            else
            {
                arrayRecords = NeoPropertyArrayStore.allocateFromNumbers( value, Collections.<DynamicRecord>emptyList(), allocator );
            }

            setSingleBlockValue( block, keyId, PropertyType.ARRAY, first( arrayRecords ).getId() );
            for ( DynamicRecord valueRecord : arrayRecords )
            {
                block.addValueRecord( valueRecord );
            }
        }
        else
        {
            throw new IllegalArgumentException( "Unknown property type on: " + value + ", " + value.getClass() );
        }
    }
    
    public static Collection<PropertyRecord> getPropertyRecordChain( RecordStore propertyStore, long firstRecordId )
    {
        long nextProp = firstRecordId;
        List<PropertyRecord> toReturn = new LinkedList<>();
        if ( nextProp == Record.NO_NEXT_PROPERTY.intValue() )
        {
            return null;
        }
        byte[] data = new byte[propertyStore.getRecordSize()];
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propertyStore.getRecord( nextProp, data );
            PropertyRecord propRecord = getRecord( nextProp, data, RecordLoad.NORMAL );
            toReturn.add(propRecord);
            nextProp = propRecord.getNextProp();
        }
        return toReturn;
    }


     static private void setSingleBlockValue( PropertyBlock block, int keyId, PropertyType type, long longValue )
    {
        block.setSingleBlock( keyId | (((long) type.intValue()) << 24)
                | (longValue << 28) );
    }
    
    @Deprecated
    public static void ensureHeavy( PropertyBlock block, RecordStore dynamicStore, long blockId, DynamicRecord.Type type )
    {
        if ( block.isLight() )
        {
            Collection<DynamicRecord> dynamicRecords = NeoDynamicStore.getRecords( dynamicStore, blockId, RecordLoad.NORMAL, type );
            for ( DynamicRecord stringRecord : dynamicRecords )
            {
                block.addValueRecord( stringRecord );
            }
        }
        else
        {
            for ( DynamicRecord dynamicRecord : block.getValueRecords() )
                NeoDynamicStore.ensureHeavy( dynamicRecord, dynamicStore );
        }
    }
    
    public static Iterable<NodePropertyUpdate> convertPhysicalToLogical(
            PropertyRecord before, long[] labelsBefore,
            PropertyRecord after, long[] labelsAfter, RecordStore stringStore, RecordStore arrayStore )
    {
        assert before.getNodeId() == after.getNodeId() :
            "Node ids differ between before(" + before.getNodeId() + ") and after(" + after.getNodeId() + ")";
        long nodeId = before.getNodeId();
        Map<Integer, PropertyBlock> beforeMap = mapBlocks( before );
        Map<Integer, PropertyBlock> afterMap = mapBlocks( after );

        @SuppressWarnings( "unchecked" )
        Set<Integer> allKeys = union( beforeMap.keySet(), afterMap.keySet() );

        Collection<NodePropertyUpdate> result = new ArrayList<>();
        for ( int key : allKeys )
        {
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );
            NodePropertyUpdate update = null;

            if ( beforeBlock != null && afterBlock != null )
            {
                // CHANGE
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                {
                    Object beforeVal = valueOf( beforeBlock, arrayStore, arrayStore );
                    Object afterVal = valueOf( afterBlock, arrayStore, arrayStore );
                    update = NodePropertyUpdate.change( nodeId, key, beforeVal, labelsBefore, afterVal, labelsAfter );
                }
            }
            else
            {
                // ADD/REMOVE
                if ( afterBlock != null )
                {
                    update = NodePropertyUpdate.add( nodeId, key, valueOf( afterBlock, arrayStore, arrayStore ), labelsAfter );
                }
                else if ( beforeBlock != null )
                {
                    update = NodePropertyUpdate.remove( nodeId, key, valueOf( beforeBlock, arrayStore, arrayStore ), labelsBefore );
                }
                else
                {
                    throw new IllegalStateException( "Weird, an update with no property value for before or after" );
                }
            }

            if ( update != null)
            {
                result.add( update );
            }
        }
        return result;
    }
    
    private static <T> Set<T> union( Set<T>... sets )
    {
        Set<T> union = new HashSet<>();
        for ( Set<T> set : sets )
        {
            union.addAll( set );
        }
        return union;
    }

    private static Map<Integer, PropertyBlock> mapBlocks( PropertyRecord before )
    {
        HashMap<Integer, PropertyBlock> map = new HashMap<>();
        for ( PropertyBlock block : before.getPropertyBlocks() )
        {
            map.put( block.getKeyIndexId(), block );
        }
        return map;
    }

    private static Object valueOf( PropertyBlock block, RecordStore stringStore, RecordStore arrayStore )
    {
        if ( block == null )
        {
            return null;
        }
        if ( block.getType() == PropertyType.STRING )
        {
            return block.getType().getValue( block, stringStore );
        }
        else if ( block.getType() == PropertyType.ARRAY )
        {
            return block.getType().getValue( block, arrayStore );
        }
        else
        {
            return block.getType().getValue( block, null );           
        }
    }
    
    
//    public String getStringFor( PropertyBlock propertyBlock )
//    {
//        ensureHeavy( propertyBlock );
//        return getStringFor( propertyBlock.getValueRecords() );
//    }

//    public String getStringFor( Collection<DynamicRecord> dynamicRecords, RecordStore stringStore )
//    {
//        Pair<byte[], byte[]> source = NeoNeoStore.readFullByteArray( dynamicRecords, PropertyType.STRING );
//        // A string doesn't have a header in the data array
//        return decodeString( source.other() );
//    }

//    public Object getArrayFor( PropertyBlock propertyBlock )
//    {
//        ensureHeavy( propertyBlock );
//        return getArrayFor( propertyBlock.getValueRecords() );
//    }

//    public Object getArrayFor( Iterable<DynamicRecord> records )
//    {
//        return getRightArray( arrayPropertyStore.readFullByteArray( records, PropertyType.ARRAY ) );
//    }
}
