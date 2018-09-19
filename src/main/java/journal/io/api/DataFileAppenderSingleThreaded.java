/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package journal.io.api;

import journal.io.api.Journal.WriteBatch;
import journal.io.api.Journal.WriteCommand;
import journal.io.api.Journal.WriteFuture;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static journal.io.util.LogHelper.warn;

/**
 * File writer to do batch appends to a data file, based on a non-blocking,
 * mostly lock-free, algorithm to maximize throughput on concurrent writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAppenderSingleThreaded implements IDataFileAppender {

    private final int SPIN_RETRIES = 100;
    private final int SPIN_BACKOFF = 10;
    //
    private final AtomicReference<Exception> asyncException = new AtomicReference<Exception>();
    private final AtomicBoolean batching = new AtomicBoolean(false);
    private volatile boolean opened;
    //
    private final Journal journal;
    //
    private volatile WriteBatch nextWriteBatch;
    private volatile DataFile lastAppendDataFile;
    private volatile RandomAccessFile lastAppendRaf;

    DataFileAppenderSingleThreaded(Journal journal) {
        this.journal = journal;
    }

    public Location storeItem(byte[] data, byte type, boolean sync, WriteCallback callback) throws IOException {
        int size = Journal.RECORD_HEADER_SIZE + data.length;

        Location location = new Location();
        location.setSize(size);
        location.setType(type);
        location.setWriteCallback(callback);
        WriteCommand write = new WriteCommand(location, data, sync);
        location = runWrite(write);

        return location;
    }

    public Future<Boolean> sync() throws ClosedJournalException, IOException {
        int spinnings = 0;
        int limit = SPIN_RETRIES;
        while (true) {
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            try {
                if (!opened) {
                    throw new ClosedJournalException("The journal is closed!");
                }
                if (batching.compareAndSet(false, true)) {
                    try {
                        Future result = null;
                        if (nextWriteBatch != null) {
                            result = new WriteFuture(nextWriteBatch.getLatch());
                            runBatch(nextWriteBatch);
                            nextWriteBatch = null;
                        } else {
                            result = new WriteFuture(journal.getLastAppendLocation().getLatch());
                        }
                        return result;
                    } finally {
                        batching.set(false);
                    }
                } else {
                    // Spin waiting for new batch ...
                    if (spinnings <= limit) {
                        spinnings++;
                        continue;
                    } else {
                        Thread.sleep(SPIN_BACKOFF);
                        continue;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }

    private Location runWrite(WriteCommand writeRecord) throws ClosedJournalException, IOException {
        WriteBatch currentBatch = null;
        while (true) {
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            if (!opened) {
                throw new ClosedJournalException("The journal is closed!");
            }
            try {
                if (nextWriteBatch == null) {
                    DataFile file = journal.getCurrentWriteDataFile();
                    boolean canBatch = false;
                    currentBatch = new WriteBatch(file, journal.getLastAppendLocation().getPointer() + 1,false);
                    canBatch = currentBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                    if (!canBatch) {
                        file = journal.newDataFile();
                        currentBatch = new WriteBatch(file, 0,false);
                    }
                    WriteCommand controlRecord = currentBatch.prepareBatch();
                    writeRecord.getLocation().setDataFileId(file.getDataFileId());
                    writeRecord.getLocation().setPointer(currentBatch.incrementAndGetPointer());
                    writeRecord.getLocation().setLatch(currentBatch.getLatch());
                    currentBatch.appendBatch(writeRecord);
                    if (!writeRecord.isSync()) {
                        nextWriteBatch = currentBatch;
                    } else {
                        runBatch(currentBatch);
                    }
                    journal.setLastAppendLocation(writeRecord.getLocation());
                    break;
                } else {
                    boolean canBatch = nextWriteBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                    writeRecord.getLocation().setDataFileId(nextWriteBatch.getDataFile().getDataFileId());
                    writeRecord.getLocation().setPointer(nextWriteBatch.incrementAndGetPointer());
                    writeRecord.getLocation().setLatch(nextWriteBatch.getLatch());
                    if (canBatch && !writeRecord.isSync()) {
                        nextWriteBatch.appendBatch(writeRecord);
                        journal.setLastAppendLocation(writeRecord.getLocation());
                        break;
                    } else if (canBatch && writeRecord.isSync()) {
                        nextWriteBatch.appendBatch(writeRecord);
                        journal.setLastAppendLocation(writeRecord.getLocation());
                        runBatch(nextWriteBatch);
                        nextWriteBatch = null;
                        break;
                    } else {
                        runBatch(nextWriteBatch);
                        nextWriteBatch = null;
                    }
                }
            } finally {
                batching.set(false);
            }
        }
        return writeRecord.getLocation();
    }

    public void open() {
        opened = true;
    }

    public void close() throws IOException {
        try {
            opened = false;
            if (nextWriteBatch != null) {
                runBatch(nextWriteBatch);
                nextWriteBatch.getLatch().await();
                nextWriteBatch = null;
            }
            journal.setLastAppendLocation(null);
            if (lastAppendRaf != null) {
                lastAppendRaf.close();
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    public Exception getAsyncException() {
        return asyncException.get();
    }


    private void runBatch(WriteBatch wb) {
        try {
            if (wb != null) {
                if (!wb.isEmpty()) {
                    boolean newOrRotated = lastAppendDataFile != wb.getDataFile();
                    if (newOrRotated) {
                        if (lastAppendRaf != null) {
                            lastAppendRaf.close();
                        }
                        lastAppendDataFile = wb.getDataFile();
                        lastAppendRaf = lastAppendDataFile.openRandomAccessFile();
                    }

                    // Perform batch:
                    Location batchLocation = wb.perform(lastAppendRaf, journal.isChecksum(), journal.isPhysicalSync(), journal.getReplicationTarget());

                    // Add batch location as hint:
                    journal.getHints().put(batchLocation, batchLocation.getThisFilePosition());

                    // Adjust journal length:
                    journal.addToTotalLength(wb.getSize());

                    // Now that the data is on disk, notify callbacks and remove the writes from the in-flight cache:
                    for (WriteCommand current : wb.getWrites()) {
                        try {
                            current.getLocation().getWriteCallback().onSync(current.getLocation());
                        } catch (Throwable ex) {
                            warn(ex, ex.getMessage());
                        }
                    }

                    // Finally signal any waiting threads that the write is on disk.
                    wb.getLatch().countDown();
                }
                // Poll next batch:
            }
        } catch (Exception ex) {
            // Notify error to all locations of all batches, and signal waiting threads:
                for (WriteCommand currentWrite : wb.getWrites()) {
                    try {
                        currentWrite.getLocation().getWriteCallback().onError(currentWrite.getLocation(), ex);
                    } catch (Throwable innerEx) {
                        warn(innerEx, innerEx.getMessage());
                    }
                }
            // Propagate exception:
            asyncException.compareAndSet(null, ex);
        }
    }
}
