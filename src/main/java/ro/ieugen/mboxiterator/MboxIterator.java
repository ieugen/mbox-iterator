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
import java.io.*;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides an iterator over email messages inside an mbox file.
 *
 * @author Ioan Eugen Stan <stan.ieugen@gmail.com>
 */
public class MboxIterator implements Iterable<CharBuffer>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MboxIterator.class);
    private final FileInputStream theFile;
    private final CharBuffer mboxCharBuffer;
    private Matcher fromLineMathcer;
    private boolean fromLineFound;
    private final MappedByteBuffer byteBuffer;
    private final CharsetDecoder DECODER;
    /** Change to true in the final invocation so bytes at teh end of the input are
     * decoding is done properly and if incmplete will cause a return of mall-formed input.
     */
    private boolean endOfInputFlag = false;
    private final int maxMessageSize;
    private final Pattern MESSAGE_START;
    private int findStart = -1;
    private int findEnd = -1;

    private MboxIterator(final File mbox,
                         final Charset charset,
                         final String regexpPattern,
                         final int regexpFlags,
                         final int MAX_MESSAGE_SIZE)
            throws FileNotFoundException, IOException, CharConversionException {
        LOG.info("Opening file {}", mbox.getAbsolutePath());
        //TODO: do better exception handling - try to process ome of them maybe?
        this.maxMessageSize = MAX_MESSAGE_SIZE;
        this.MESSAGE_START = Pattern.compile(regexpPattern, regexpFlags);
        this.DECODER = charset.newDecoder();
        this.mboxCharBuffer = CharBuffer.allocate(MAX_MESSAGE_SIZE);
        this.theFile = new FileInputStream(mbox);
        this.byteBuffer = theFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, theFile.getChannel().size());
        initMboxIterator();
    }

    private void initMboxIterator() throws IOException, CharConversionException {
        logBufferDetails(byteBuffer);
        decodeNextCharBuffer();
        logBufferDetails(mboxCharBuffer);
        fromLineMathcer = MESSAGE_START.matcher(mboxCharBuffer);
        fromLineFound = fromLineMathcer.find();
        if (fromLineFound) {
            saveFindPositions(fromLineMathcer);
        } else {
            throw new IllegalArgumentException("File does not contain From_ lines! Maybe not be a vaild Mbox.");
        }
    }

    private void decodeNextCharBuffer() throws CharConversionException {
        CoderResult coderResult = DECODER.decode(byteBuffer, mboxCharBuffer, endOfInputFlag);
        updateEndOfInputFlag();
        mboxCharBuffer.flip();
        if (coderResult.isError()) {
            if (coderResult.isMalformed()) {
                throw new CharConversionException("Malformed input!");
            } else if (coderResult.isUnmappable()) {
                throw new CharConversionException("Unmappable character!");
            }
        }
    }

    private void updateEndOfInputFlag() {
        if (byteBuffer.remaining() <= maxMessageSize) {
            endOfInputFlag = true;
        }
    }

    private void saveFindPositions(Matcher lineMatcher) {
        findStart = lineMatcher.start();
        findEnd = lineMatcher.end();
    }

    @Override
    public Iterator<CharBuffer> iterator() {
        return new MessageIterator();
    }

    @Override
    public void close() throws IOException {
        theFile.close();
    }

    /**
     * Utility method to log important details about buffers.
     * @param buffer
     */
    private static void logBufferDetails(final Buffer buffer) {
        LOG.info("Buffer details: "
                + "\ncapacity:\t" + buffer.capacity()
                + "\nlimit:\t" + buffer.limit()
                + "\nremaining:\t" + buffer.remaining()
                + "\nposition:\t" + buffer.position()
                + "\nis direct:\t" + buffer.isDirect()
                + "\nhas array:\t" + buffer.hasArray()
                + "\nbuffer:\t" + buffer.isReadOnly()
                + "\nclass:\t" + buffer.getClass());
    }

    private class MessageIterator implements Iterator<CharBuffer> {

        @Override
        public boolean hasNext() {
            return fromLineFound;
        }

        /**
         * Returns a CharBuffer instance that contains a message between position and limit.
         * The array that backs this instance is the whole block of decoded messages.
         * @return CharBuffer instance
         */
        @Override
        public CharBuffer next() {
            LOG.info("next() called at offset {}", fromLineMathcer.start());
            CharBuffer message = mboxCharBuffer.slice();
            message.position(fromLineMathcer.end() + 1);
            logBufferDetails(message);
            fromLineFound = fromLineMathcer.find();
            if (fromLineFound) {
                LOG.info("We limit the buffer to {} ?? {}", fromLineMathcer.start(), fromLineMathcer.end());
                saveFindPositions(fromLineMathcer);
                message.limit(fromLineMathcer.start());
            } else {
                LOG.info("No more From_ lines in this buffer. Bytes remaining {}", byteBuffer.remaining());
                /* We didn't find other From_ lines this means either:
                 *  - we reached end of mbox and no more messages
                 *  - we reached end of CharBuffer and need to decode another batch.
                 */
                if (byteBuffer.hasRemaining()) {
                    // decode another batch, but remember to copy the remaining chars first
                    CharBuffer oldData = mboxCharBuffer.duplicate();
                    mboxCharBuffer.clear();
                    logBufferDetails(mboxCharBuffer);
                    oldData.position(findEnd + 1);// asda
                    logBufferDetails(oldData);
                    while (oldData.hasRemaining()) {
                        mboxCharBuffer.put(oldData.get());
                    }
                    logBufferDetails(mboxCharBuffer);
                    try {
                        decodeNextCharBuffer();
                    } catch (CharConversionException ex) {
                        throw new RuntimeException(ex);
                    }
                    fromLineMathcer = MESSAGE_START.matcher(mboxCharBuffer);
                    fromLineFound = fromLineMathcer.find();
                    message = mboxCharBuffer.slice();
                    fromLineFound = fromLineMathcer.find();
                    if (fromLineFound) {
                        message.limit(fromLineMathcer.start());
                    }
                }
            }
            logBufferDetails(message);
            return message;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public static Builder fromFile(File filePath) {
        return new Builder(filePath);
    }

    public static Builder fromFile(String file) {
        return new Builder(file);
    }

    protected static class Builder {

        private final File file;
        private Charset charset = Charsets.UTF_8;
        private String regexpPattern = "^From \\S+@\\S.*\\d{4}$";
        private int flags = Pattern.MULTILINE;
        // default max message size in chars: 10k chars.
        private int maxMessageSize = 10 * 1024;

        private Builder(String filePath) {
            this(new File(filePath));
        }

        private Builder(File file) {
            this.file = file;
        }

        public Builder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Builder fromLine(String fromLine) {
            this.regexpPattern = fromLine;
            return this;
        }

        public Builder flags(int flags) {
            this.flags = flags;
            return this;
        }

        public Builder maxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public MboxIterator build() throws FileNotFoundException, IOException {
            return new MboxIterator(file, charset, regexpPattern, flags, maxMessageSize);
        }
    }
}
