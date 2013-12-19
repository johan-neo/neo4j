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

import static junit.framework.Assert.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule.uniquenessConstraintRule;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.ByteBuffer;

import javax.transaction.xa.XAException;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.HitStat;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.RecordStore;
import org.neo4j.kernel.impl.nioneo.alt.Sweeper;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

public class IntegrityValidatorTest
{

    @Test
    public void shouldValidateUniquenessIndexes() throws Exception
    {
        // Given
        FlatNeoStores store = mock( FlatNeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator(store, indexes);

        doThrow( new ConstraintVerificationFailedKernelException( null, new RuntimeException() ))
         .when( indexes ).validateIndex( 2l );

        UniquenessConstraintRule record = uniquenessConstraintRule( 1l, 1, 1, 2l );

        // When
        try
        {
            validator.validateSchemaRule( record );
            fail("Should have thrown integrity error.");
        }
        catch(XAException e)
        {
            assertThat(e.errorCode, equalTo(XAException.XA_RBINTEGRITY));
        }
    }

    @Test
    public void deletingNodeWithRelationshipsIsNotAllowed() throws Exception
    {
        // Given
        FlatNeoStores stores = mock( FlatNeoStores.class );
        IndexingService indexes = mock(IndexingService.class);
        IntegrityValidator validator = new IntegrityValidator(stores, indexes );

        NodeRecord record = new NodeRecord( 1l, 1l, -1l );
        record.setInUse( false );

        // When
        try
        {
            validator.validateNodeRecord( record );
            fail("Should have thrown integrity error.");
        }
        catch(XAException e)
        {
            assertThat(e.errorCode, equalTo(XAException.XA_RBINTEGRITY));
        }
    }

    @Test
    public void transactionsStartedBeforeAConstraintWasCreatedAreDisallowed() throws Exception
    {
        // Given
        FlatNeoStores stores = mock( FlatNeoStores.class );
        NeoNeoStore neoStore = mock( NeoNeoStore.class );
        RecordStore recordStore = new FakeRecordStore( 10l );
        when( neoStore.getRecordStore() ).thenReturn( recordStore );
        
        when( stores.getNeoStore() ).thenReturn( neoStore );
        IndexingService indexes = mock(IndexingService.class);
        // when(stores.getLatestConstraintIntroducingTx()).thenReturn( 10l );
        IntegrityValidator validator = new IntegrityValidator( stores, indexes );

        // When
        try
        {
            validator.validateTransactionStartKnowledge( 1 );
            fail("Should have thrown integrity error.");
        }
        catch(XAException e)
        {
            assertThat(e.errorCode, equalTo(XAException.XA_RBINTEGRITY));
        }
    }
    
    private static class FakeRecordStore implements RecordStore
    {
        private long value;

        public FakeRecordStore( long value )
        {
            this.value = value;
        }

        @Override
        public void getRecord( long record, byte[] data )
        {
            if ( record == NeoNeoStore.LATEST_CONSTRAINT_TX_POSITION )
            {
                ByteBuffer.wrap( data ).putLong( value );
            }
        }

        @Override
        public void writeRecord( long record, byte[] data )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getRecordSize()
        {
            return NeoNeoStore.RECORD_SIZE;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void force()
        {
        }

        @Override
        public HitStat sweep( Sweeper sweeper )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeOutDirtyPages()
        {
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return 6;
        }

        @Override
        public long getStoreSizeInBytes()
        {
            return getRecordSize()*getHighestPossibleIdInUse();
        }
    }
}
