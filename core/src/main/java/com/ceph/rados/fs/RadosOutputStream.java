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

import java.io.OutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.fs.INode.FileType;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;

public class RadosOutputStream extends OutputStream {

    private static IoCTX ioctx;
    private boolean closed;
    private String oid;
    private long pos = 0;

    private static final Log LOG = 
        LogFactory.getLog(RadosOutputStream.class.getName());
    
    
    public RadosOutputStream(IoCTX io, String id) {
        ioctx = io;
        oid = id;
        closed = false;
    }

    public RadosOutputStream(RadosFileSystemStore store, String id) {
        oid = id;
        closed = false;
    }


    @Override
    public synchronized void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        byte[] buf = new byte[4];
        try {
            ioctx.write(oid, buf, pos);
            pos ++;
        } catch (Exception e) {
            throw new IOException("write failed");
        }
    }

    @Override
    public synchronized void write(byte buf[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        try {
            ioctx.write(oid, buf, off);
            pos += buf.length;
        } catch (Exception e) {
            throw new IOException("write failed");
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
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
}
