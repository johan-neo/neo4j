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

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.alt.Store;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;

public interface NodeLabels
{
    long[] get( Store labelStore );

    long[] getIfLoaded();

    Collection<DynamicRecord> put( long[] labelIds, Store labelStore );

    Collection<DynamicRecord> add( long labelId, Store labelStore );

    Collection<DynamicRecord> remove( long labelId, Store labelStore );

    boolean isInlined();

    void ensureHeavy( Store labelStore );
}
