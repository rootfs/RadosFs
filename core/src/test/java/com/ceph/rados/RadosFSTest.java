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
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.ByteArrayInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.Path;
import com.ceph.rados.fs.INode.FileType;
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
    private void GetVersion() throws IOException {
        String s = store.getVersion();
        System.out.println("version: " + s + " tested");
    }

    /**
     * List paths
     */
    private void ListPath(String root) throws IOException {
        Set<Path> p = store.listSubPaths(new Path(root));
        Path[] s = p.toArray(new Path[0]);
        for (int i = 0; i < s.length; i++) {
            System.out.println("Path " + i + ": " + s[i].toString());
        }

    }
    /**
     * Dump rados pool
     */
    private void Dump() throws IOException {
        // ensure we have a valid pool
        store.dump();
    }

    private void Mkdirs(String path) throws IOException {
        store.storeINode(new Path(path), INode.DIRECTORY_INODE);
    }

    @Test
    public void testMkdirs() throws IOException {
        GetVersion();
        Mkdirs("/");
        Mkdirs("/test");
        Mkdirs("/test/dir_test");
        ListPath("/");
        Dump();
    }

    @Test
    public void testFileExists() throws IOException {
        final String f = "/test/file_test/fileio";
        Mkdirs("/");
        Mkdirs("/test");
        Mkdirs("/test/file_test");
        Path p = new Path(f);
        if (store.inodeExists(p)) {
            System.out.println("Inode " + f + " exists, delete first");
            store.deleteINode(new Path(f));
        }
        INode inode = new INode(FileType.FILE, null);
        store.storeINode(p, inode);
        ListPath("/");
        Dump();
    }

    @Test
    public void testFileCopy() throws IOException, Exception {
        final String f = "/test/file_test/filecopy";
        final File infile = new File("/etc/passwd");
        Mkdirs("/");
        Mkdirs("/test");
        Mkdirs("/test/file_test");
        Path p = new Path(f);
        if (store.inodeExists(p)) {
            System.out.println("Inode " + f + " exists, delete first");
            store.deleteINode(new Path(f));
        }
        Block block = store.createAndStoreBlock(infile);
        List<Block> blocks = new ArrayList<Block>();
        blocks.add(block);
        INode inode = new INode(FileType.FILE, 
                                blocks.toArray(new Block[blocks.size()]));
        store.storeINode(p, inode);
        Dump();
    }

    @Test
    public void testFileWrite() throws IOException, Exception {
        final String f = "/test/file_test/filewrite";
        final String data = "blah blah blalalah";
        Mkdirs("/");
        Mkdirs("/test");
        Mkdirs("/test/file_test");
        Path p = new Path(f);
        if (store.inodeExists(p)) {
            System.out.println("Inode " + f + " exists, delete first");
            store.deleteINode(new Path(f));
        }
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        Block block = store.createAndStoreBlock(in);
        List<Block> blocks = new ArrayList<Block>();
        blocks.add(block);
        INode inode = new INode(FileType.FILE, 
                                blocks.toArray(new Block[blocks.size()]));
        store.storeINode(p, inode);
        Dump();
    }


}
