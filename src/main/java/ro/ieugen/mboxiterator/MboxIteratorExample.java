/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package ro.ieugen.mboxiterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

/**
 * Simple example of how to use MboxIterator. We split one Mbox file into
 * individual email messages.
 *
 * @author Ioan Eugen Stan <stan.ieugen@gmail.com>
 */
public class MboxIteratorExample {

    private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private final static CharsetEncoder ENCODER = UTF8_CHARSET.newEncoder();

    // simple example of how to split an mbox into individual files
    public static void main(String[] args) throws IOException, FileNotFoundException {
        final File mbox = new File("/home/ieugen/contracte/firimituri/gmane.test.yahoo/test-utf");
        long start = System.currentTimeMillis();
        int count = 0;
        for (CharBuffer buf : new MboxIterator.Builder(mbox).build()) {
            FileOutputStream fout = new FileOutputStream(new File("target/messages/msg-" + count));
            FileChannel fileChannel = fout.getChannel();
            ByteBuffer buf2 = ENCODER.encode(buf);
            fileChannel.write(buf2);
            fileChannel.close();
            fout.close();
            count++;
        }
        System.out.println("Found " + count + " messages");
        long end = System.currentTimeMillis();
        System.out.println("Done in: " + (end - start) + " milis");
    }
}
