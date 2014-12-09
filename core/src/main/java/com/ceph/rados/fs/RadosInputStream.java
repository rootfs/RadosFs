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

import java.io.InputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.fs.INode.FileType;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;

public class RadosInputStream extends InputStream {

    private static IoCTX ioctx;
    private boolean closed;
    private long size = -1;
    private long pos = 0;
    private String oid;

    private static final Log LOG = 
        LogFactory.getLog(RadosInputStream.class.getName());
    
    
    public RadosInputStream(IoCTX io, String id) {
        ioctx = io;
        oid = id;
        closed = false;
    }

    public RadosInputStream(RadosFileSystemStore store, String id) {
        oid = id;
        closed = false;
    }

    private synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int available() throws IOException {
        try {
            if (size < 0 ) {
                size = ioctx.stat(oid).getSize();
            }
            return (int) (size - pos);
        } catch (Exception e) {
            throw new IOException("stat failed");
        }
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            if (size < 0 ) {
                size = ioctx.stat(oid).getSize();
            }
        } catch (Exception e) {
            throw new IOException("stat failed");
        }
        byte[] buf = new byte[1];
        try {
            int read = ioctx.read(oid, 1, pos, buf);
            if (read > 0)
                pos ++;
            return buf[0];
        } catch (Exception e) {
            throw new IOException("read failed");
        }
    }

    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            int read = ioctx.read(oid, len, off, buf);
            if (read > 0) {
                pos += read;
            }
            return read;
        } catch (Exception e) {
            throw new IOException("read failed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        closed = true;
    }

    /**
     * We don't support marks.
     */
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }

}
