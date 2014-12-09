/*
 * RADOS Java - Java bindings for librados
 *
 * Copyright (C) 2013 Wido den Hollander <wido@42on.com>
 * Copyright (C) 2014 1&1 - Behar Veliqi <behar.veliqi@1und1.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.ceph.rados.fs;

import java.io.IOException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.Path;

public final class RadosFSTest {

    private static String ENV_CONFIG_FILE = System.getenv("RADOS_JAVA_CONFIG_FILE");
    private static String ENV_ID = System.getenv("RADOS_JAVA_ID");
    private static String ENV_POOL = System.getenv("RADOS_JAVA_POOL");

    private static final String CONFIG_FILE = ENV_CONFIG_FILE == null ? "/etc/ceph/ceph.conf" : ENV_CONFIG_FILE;
    private static final String ID = ENV_ID == null ? "admin" : ENV_ID;
    private static final String POOL = ENV_POOL == null ? "data" : ENV_POOL;

    private static RadosFileSystemStore store;

    @BeforeClass
    public static void setUp() throws Exception {
        store = new RadosFileSystemStore();
        store.initialize(CONFIG_FILE, ID, POOL);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        store.tearDown();
    }

    /**
     * This test verifies if we can get the version out of librados
     * It's currently hardcoded to expect at least 0.48.0
     */
    @Test
    public void testGetVersion() throws IOException {
        String s = store.getVersion();
        System.out.println("version: " + s + " tested");
    }

    /**
     * List paths
     */
    @Test
    public void testListPath() throws IOException {
        Set<Path> p = store.listSubPaths(new Path("/"));
        String[] s = p.toArray(new String[0]);
        System.out.println("size: " + s.length);
        for (int i = 0; i < s.length; i++) {
            System.out.println(i + ": " + s[i]);
        }

    }
    /**
     * Dump rados pool
     */
    @Test
    public void testDump() throws IOException {
        // ensure we have a valid pool
        store.retrieveINode(new Path("/"));
        store.dump();
    }


}
