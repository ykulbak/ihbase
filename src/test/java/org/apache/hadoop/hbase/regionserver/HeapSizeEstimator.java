/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A simple utility to help estimate the heap size of indexes.
 */
public class HeapSizeEstimator {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java o.a.h.h.HeapSizeEstimator [num entries]");
        }

        int entries = Integer.parseInt(args[0]);

        final byte[] family = Bytes.toBytes("family");
        final byte[] qualifier = Bytes.toBytes("qualifier");
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
        CompleteIndexBuilder completeIndexBuilder = new CompleteIndexBuilder(columnDescriptor, new IdxIndexDescriptor(qualifier, IdxQualifierType.LONG));
        for (long i = 0; i < entries; i++) {
            completeIndexBuilder.addKeyValue(new KeyValue(Bytes.toBytes(i), family, qualifier, Bytes.toBytes(i)), (int) i);
        }
        CompleteIndex ix = (CompleteIndex) completeIndexBuilder.finalizeIndex(entries);
        System.out.printf("index heap size=%d bytes, total heap=%d\n", ix.heapSize(), Runtime.getRuntime().totalMemory());
    }
}
