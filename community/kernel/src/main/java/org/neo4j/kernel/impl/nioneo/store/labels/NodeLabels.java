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
package org.neo4j.kernel.impl.nioneo.store.labels;

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.alt.NeoLabelStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;

public interface NodeLabels
{
    long[] get( NeoLabelStore labelStore );

    long[] getIfLoaded();

    Collection<DynamicRecord> put( long[] labelIds, NeoLabelStore labelStore );

    Collection<DynamicRecord> add( long labelId, NeoLabelStore labelStore );

    Collection<DynamicRecord> remove( long labelId, NeoLabelStore labelStore );

    boolean isInlined();

    void ensureHeavy( NeoLabelStore labelStore );
}
