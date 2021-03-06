/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransactionRepresentationStoreApplierTest
{

    private final IndexingService indexService = mock( IndexingService.class );
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final NeoStore neoStore = mock( NeoStore.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final LockService lockService = mock( LockService.class );
    private final LegacyIndexApplier.ProviderLookup legacyIndexProviderLookup =
            mock( LegacyIndexApplier.ProviderLookup.class );
    private final IndexConfigStore indexConfigStore = mock( IndexConfigStore.class );


    private final int transactionId = 12;

    @Test
    public void transactionRepresentationShouldAcceptApplierVisitor() throws IOException
    {
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore );

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        applier.apply( transaction, transactionId, false );

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command, IOException>>any() );
    }

    @Test
    public void shouldUpdateIdGeneratorsWhenOnRecovery() throws IOException
    {
        TransactionRepresentationStoreApplier applier = new TransactionRepresentationStoreApplier( indexService,
                labelScanStore, neoStore, cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore );

        TransactionRepresentation transaction = mock( TransactionRepresentation.class );

        applier.apply( transaction, transactionId, true );

        verify( transaction, times( 1 ) ).accept( Matchers.<Visitor<Command, IOException>>any() );
        verify( neoStore, times( 1 ) ).updateIdGenerators();
    }
}
