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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.CharBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link MboxIterator}.
 * @author estan
 */
public class MboxIteratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(MboxIteratorTest.class);
    private final TestName name = new TestName();
    public static final String MBOX_PATH = "src/test/resources/test-1/mbox.rlug";

    /**
     * Test of iterator method, of class MboxIterator.
     */
    @Test
    public void testIterator() throws FileNotFoundException, IOException {
        LOG.info("Executing {} ", name.getMethodName());
        int count = 0;
        for (CharBuffer msg : MboxIterator.fromFile(MBOX_PATH).build()) {
            char[] message = Files.toString(new File(MBOX_PATH + "-" + count), Charsets.UTF_8).toCharArray();
            char[] ourMsg = new char[msg.length()];
            msg.mark();
            msg.get(ourMsg);
            msg.reset();
            printCharBuffer(msg);
            Assert.assertArrayEquals("Missmatch with file " + count, message, ourMsg);
            count++;
        }
    }

    private void printCharBuffer(CharBuffer msg) {
        System.out.println("---------------------------------------------------------------------------- \n" + msg
                + "----------------------------------------------------------------------------");
    }
}
