/**
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

package com.ceph.rados.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Random;

import org.apache.hadoop.fs.Path;

import com.ceph.rados.fs.INode.FileType;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;

/**
 * A class of storing and retrieving {@link INode}s and {@link Block}s.
 */
public class RadosFileSystemStore {
    private static final String FILE_SYSTEM_VERSION_NAME = "fs-version";
    private static final String FILE_SYSTEM_VERSION_VALUE = "1";

    private static final String PATH_DELIMITER = Path.SEPARATOR;
    private static final String BLOCK_PREFIX = "block_";

    private static String CONFIG_FILE;
    private static String ID;
    private static String POOL;

    private static Rados rados;
    private static IoCTX ioctx = null;

    public void initialize(String conf, String id, String pool) throws IOException {    
        CONFIG_FILE =  conf == null ? "/etc/ceph/ceph.conf" : conf;
        ID = id == null ? "admin" : id;
        POOL = pool == null ? "data" : pool;
        // create rados and ioctx
        rados = new Rados(ID);
        try {
            rados.confReadFile(new File(CONFIG_FILE));
            rados.connect();
            ioctx = rados.ioCtxCreate(POOL);
        } catch (Exception e) {
            throw new IOException("rados init failed");
        }
    }

    public void tearDown() throws Exception {
        //FIXME: pending activities?
        rados.shutDown();
        rados.ioCtxDestroy(ioctx);
    }

    public String getVersion() throws IOException {
        int[] version = rados.getVersion();
        return Integer.toString(version[0]) + "." + Integer.toString(version[1]) + "." + Integer.toString(version[2]);
    }

    public IoCTX getIoCTX() {
        return ioctx;
    }

    private void delete(String key) throws IOException {
        try { 
            ioctx.remove (key);
        } catch (Exception e) {
            throw new IOException("delete failed");
        }
    }

    public void deleteINode(Path path) throws IOException {
        delete(pathToKey(path));
    }

    public void deleteBlock(Block block) throws IOException {
        delete(blockToKey(block));
    }

    public boolean inodeExists(Path path) throws IOException {
        String key = pathToKey(path);
        try {
            RadosObjectInfo info = get(key);
            if (info == null) {
                if (isRoot(key)) {
                    storeINode(path, INode.DIRECTORY_INODE);
                    return true;
                } else {
                    return false;
                }
            }
        } catch (Exception e) {
            throw new IOException("store inode failed");
        }

        return true;
    }
  
    public boolean blockExists(long blockId) throws IOException {
        try {
            RadosObjectInfo info = get(blockToKey(blockId));
            if (info == null) {
                return false;
            }
        } catch (Exception e) {
            throw new IOException("block get failed");
        }

        return true;
    }

    private RadosObjectInfo get(String key)
        throws Exception {
        RadosObjectInfo info = null;
        try {
            info = ioctx.stat(key);
            return info;
        } catch (Exception e) {
            return null;
        }
    }

    private RadosObjectInfo get(String key, long byteRangeStart) throws IOException {
        RadosObjectInfo info = null;
        try {
            info = ioctx.stat(key);
            if (info.getSize() > byteRangeStart) {
                return info;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public INode retrieveINode(Path path) throws IOException {
        String key = pathToKey(path);
        try {
            RadosObjectInfo info = get(key);

            if (info == null && isRoot(key)) {
                storeINode(path, INode.DIRECTORY_INODE);
                return INode.DIRECTORY_INODE;
            }
            return INode.deserialize(new RadosInputStream(ioctx, info.getOid()));
        } catch (Exception e) {
            throw new IOException("get inode failed");
        }
    }

    public byte[] retrieveBlock(Block block, long byteRangeStart)
        throws IOException {
        RadosObjectInfo info = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            info = get(blockToKey(block), byteRangeStart);
            if (null == info) {
                throw new IOException("no such object");
            }
            in = new RadosInputStream(ioctx, info.getOid());
            out = new RadosOutputStream(ioctx, info.getOid());
            int bufferSize = in.available() - (int)byteRangeStart;
            byte[] buf = new byte[bufferSize];
            int numRead;
            while ((numRead = in.read(buf)) > 0) {
                out.write(buf, 0, numRead);
            }
            return buf;
        } catch (IOException e) {
            // close output stream to file then delete file
            closeQuietly(out);
            out = null; // to prevent a second close
            throw e;
        } finally {
            closeQuietly(out);
            closeQuietly(in);
        }
    }
  
    public Set<Path> listSubPaths(Path path) throws IOException {
        String prefix = pathToKey(path);
        if (!prefix.endsWith(PATH_DELIMITER)) {
            prefix += PATH_DELIMITER;
        }
        try {
            String[] objects = ioctx.listObjects();
            Set<Path> prefixes = new TreeSet<Path>();
            for (int i = 0; i < objects.length; i++) {
                if (objects[i].startsWith(prefix)) {
                    prefixes.add(keyToPath(objects[i]));
                }
            }
            prefixes.remove(path);
            return prefixes;
        } catch (Exception e) {
            throw new IOException("list objects failed");
        }

    }
  
    public Set<Path> listDeepSubPaths(Path path) throws IOException {
        return listSubPaths(path);
    }

    private void put(String key, InputStream in, long length)
        throws IOException {
        try {
            byte[] buf = new byte[(int)length];
            int ret = in.read(buf, 0, (int)length);
            RadosOutputStream out = new RadosOutputStream(ioctx, key);
            if (ret > 0) {
                out.write(buf, 0, ret);
            }
        } catch (Exception e) {
            throw new IOException("Rados write failed");
        }
    }

    
    public void storeINode(Path path, INode inode) throws IOException {
        put(pathToKey(path), inode.serialize(), inode.getSerializedLength());
    }

    public void storeBlock(Block block, InputStream in, long len) throws IOException {
        put(blockToKey(block), in, len);
    }

    public void storeBlock(Block block, File file) throws IOException {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            storeBlock(block, in, block.getLength());
        } finally {
            closeQuietly(in);
        }    
    }

    public synchronized Block createAndStoreBlock(File file) throws Exception {
        Random r = new Random();
        long blockId = r.nextLong();
        while (blockExists(blockId)) {
            blockId = r.nextLong();
        }
        Block block = new Block(blockId, file.length());
        storeBlock(block, file);
        return block;
    }

    public synchronized Block createAndStoreBlock(InputStream in) throws Exception {
        Random r = new Random();
        long blockId = r.nextLong();
        while (blockExists(blockId)) {
            blockId = r.nextLong();
        }
        Block block = new Block(blockId, in.available());
        storeBlock(block, in, in.available());
        return block;
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        return path.toUri().getPath();
    }

    private Path keyToPath(String key) {
        return new Path(key);
    }
  
    private String blockToKey(long blockId) {
        return BLOCK_PREFIX + blockId;
    }

    private String blockToKey(Block block) {
        return blockToKey(block.getId());
    }

    private boolean isRoot(String key) {
        return key.isEmpty() || key.equals("/");
    }

    public void purge() throws IOException {
        try {
            String[] objects = ioctx.listObjects();
            for (int i = 0; i < objects.length; i++) {
                ioctx.remove(objects[i]);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("Rados Filesystem:\n");
        try {
            String[] objects = ioctx.listObjects();
            for (int i = 0; i < objects.length; i++) {
                if (objects[i].startsWith(BLOCK_PREFIX)) {
                    continue;
                }
                Path path = keyToPath(objects[i]);
                sb.append(path).append(":");
                INode m = retrieveINode(path);
                sb.append("\t").append(m.getFileType()).append("\n");
                if (m.getFileType() == FileType.DIRECTORY) {
                    continue;
                }
                for (int j = 0; j < m.getBlocks().length; j++) {
                    sb.append("\tBlockId: ").append(m.getBlocks()[j].getId()).append(" Length: ").append(m.getBlocks()[j].getLength()).append("\n");
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        System.out.println(sb);
    }

}
