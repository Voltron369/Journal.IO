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

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import journal.io.api.Journal.WriteBatch;
import journal.io.api.Journal.WriteCommand;
import journal.io.api.Journal.WriteFuture;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static journal.io.api.Journal.BATCH_CONTROL_RECORD_SIZE;
import static journal.io.util.LogHelper.warn;

/**
 * File writer to do batch appends to a data file, based on a non-blocking,
 * mostly lock-free, algorithm to maximize throughput on concurrent writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAppenderDisruptor implements IDataFileAppender {
    private final AtomicReference<Exception> asyncException = new AtomicReference<Exception>();
    private volatile boolean opened;
    //
    private final Journal journal;
    //
    private volatile WriteBatch nextWriteBatch;
    private volatile long sequenceId = -1;
    private volatile DataFile lastAppendDataFile;
    private volatile RandomAccessFile lastAppendRaf;
    WaitStrategy waitStrategy = new BlockingWaitStrategy();
    WaitStrategy waitStrategy2 = new BusySpinWaitStrategy();
    private final Disruptor<WriteBatch> writeBatchDisruptor;
    private final Disruptor<WriteCommand> writeCommandDisruptor;

    DataFileAppenderDisruptor(final Journal journal, int size, int size2) {
        writeBatchDisruptor = new Disruptor<>(WriteBatch.EVENT_FACTORY,size,Executors.defaultThreadFactory(),ProducerType.SINGLE,waitStrategy);
        writeCommandDisruptor = new Disruptor<>(WriteCommand.EVENT_FACTORY,size2,Executors.defaultThreadFactory(),ProducerType.MULTI,waitStrategy2);
        this.journal = journal;
        writeBatchDisruptor.handleEventsWith(new EventHandler<WriteBatch>() {
            @Override
            public void onEvent(WriteBatch writeBatch, long l, boolean b) throws Exception {
                writeBatch.finalizeBatch(journal.isChecksum());
            }
        }).then(new EventHandler<WriteBatch>() {
            @Override
            public void onEvent(WriteBatch writeBatch, long l, boolean b) throws Exception {
                runBatch(writeBatch);
            }
        } , new EventHandler<WriteBatch>() {
            @Override
            public void onEvent(WriteBatch writeBatch, long l, boolean b) throws Exception {
                //writeBatch.inflight(journal);
            }
        }).then(new EventHandler<WriteBatch>() {
            @Override
            public void onEvent(WriteBatch writeBatch, long l, boolean b) throws Exception {
                //writeBatch.inflight(journal);
                writeBatch.clear();
            }
        });
        writeCommandDisruptor.handleEventsWith(new EventHandler<WriteCommand>() {
            @Override
            public void onEvent(WriteCommand writeCommand, long l, boolean b) throws Exception {
                runWrite(writeCommand);
            }
        }).then(new EventHandler<WriteCommand>() {
            @Override
            public void onEvent(WriteCommand writeCommand, long l, boolean b) throws Exception {
                writeCommand.getLocation().getLatch().await();
            }
        });
    }

    public Location storeItem(byte[] data, byte type, boolean sync, WriteCallback callback) throws IOException {
        int size = Journal.RECORD_HEADER_SIZE + data.length;

        long sequenceId = writeCommandDisruptor.getRingBuffer().next();
        if (!writeCommandDisruptor.getRingBuffer().hasAvailableCapacity(1)) {
            sync = true;
        }
        WriteCommand write = writeCommandDisruptor.get(sequenceId);

        CountDownLatch latch = null;

        Location location = new Location();
        location.setSize(size);
        location.setType(type);
        location.setWriteCallback(callback);
        latch = new CountDownLatch(1);
        location.setLatch2(latch);
        write.setLocation(location);
        write.setData(data);
        write.setSync(sync);
        writeCommandDisruptor.getRingBuffer().publish(sequenceId);
        if (!opened) {
            throw new ClosedJournalException("The journal is closed!");
        }
        try {
            latch.await();
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            if (sync)
                location.getLatch().await();
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
        } catch (InterruptedException e) {
        }
        return location;
    }

    public Future<Boolean> sync() throws ClosedJournalException, IOException {
        int size = Journal.RECORD_HEADER_SIZE;
        long sequenceId = writeCommandDisruptor.getRingBuffer().next();
        WriteCommand write = writeCommandDisruptor.get(sequenceId);

        CountDownLatch latch=new CountDownLatch(1);;

        Location location = new Location();
        location.setLatch2(latch);
        location.setSize(size);
        location.setType(Location.SYNC_TYPE);
        location.setWriteCallback(Location.NoWriteCallback.INSTANCE);
        write.setLocation(location);
        write.setSync(true);
        writeCommandDisruptor.getRingBuffer().publish(sequenceId);
        if (!opened) {
            throw new ClosedJournalException("The journal is closed!");
        }
        try {
            latch.await();
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            location.getLatch().await();
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
        } catch (InterruptedException e) {}
        return new WriteFuture(location.getLatch());
    }

    private Location runWrite(WriteCommand writeRecord) throws ClosedJournalException, IOException {
        WriteBatch currentBatch = null;
        while (true) {
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            if (!opened) {
                asyncException.set(new ClosedJournalException("The journal is closed!"));
                throw new ClosedJournalException("The journal is closed!");
            }
            if (writeRecord.getLocation().getType() == Location.SYNC_TYPE) {
                if (nextWriteBatch == null) {
                    writeRecord.getLocation().setLatch(writeRecord.getLocation().getLatch2());
                    writeRecord.getLocation().getLatch2().countDown();
                } else {
                    writeRecord.getLocation().setLatch(nextWriteBatch.getLatch());
                    writeBatchDisruptor.getRingBuffer().publish(sequenceId);
                    writeRecord.getLocation().getLatch2().countDown();
                    nextWriteBatch = null;
                }
                break;
            }
            if (nextWriteBatch == null) {
                DataFile file = journal.getCurrentWriteDataFile();
                boolean canBatch = false;
                sequenceId = writeBatchDisruptor.getRingBuffer().next();
                currentBatch = writeBatchDisruptor.get(sequenceId);
                currentBatch.setDataFile(file);
                currentBatch.setOffset(file.getLength());
                currentBatch.setPointer(journal.getLastAppendLocation().getPointer() + 1);
                currentBatch.setSize(BATCH_CONTROL_RECORD_SIZE);
                canBatch = currentBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                if (!canBatch) {
                    file = journal.newDataFile();
                    currentBatch.setDataFile(file);
                    currentBatch.setOffset(file.getLength());
                    currentBatch.setPointer(0);
                    currentBatch.setSize(BATCH_CONTROL_RECORD_SIZE);
                }
                WriteCommand controlRecord = currentBatch.prepareBatch();
                writeRecord.getLocation().setDataFileId(file.getDataFileId());
                writeRecord.getLocation().setPointer(currentBatch.incrementAndGetPointer());
                writeRecord.getLocation().setLatch(currentBatch.getLatch());
                currentBatch.appendBatch(writeRecord);
                if (!writeRecord.isSync()) {
                    journal.getInflightWrites().put(controlRecord.getLocation(), controlRecord);
                    journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                    nextWriteBatch = currentBatch;
                } else {
                    writeBatchDisruptor.getRingBuffer().publish(sequenceId);
                }
                journal.setLastAppendLocation(writeRecord.getLocation());
                writeRecord.getLocation().getLatch2().countDown();
                break;
            } else {
                boolean canBatch = nextWriteBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                writeRecord.getLocation().setDataFileId(nextWriteBatch.getDataFile().getDataFileId());
                writeRecord.getLocation().setPointer(nextWriteBatch.incrementAndGetPointer());
                writeRecord.getLocation().setLatch(nextWriteBatch.getLatch());
                if (canBatch && !writeRecord.isSync()) {
                    nextWriteBatch.appendBatch(writeRecord);
                    journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                    journal.setLastAppendLocation(writeRecord.getLocation());
                    writeRecord.getLocation().getLatch2().countDown();
                    break;
                } else if (canBatch && writeRecord.isSync()) {
                    nextWriteBatch.appendBatch(writeRecord);
                    journal.setLastAppendLocation(writeRecord.getLocation());
                    writeBatchDisruptor.getRingBuffer().publish(sequenceId);
                    writeRecord.getLocation().getLatch2().countDown();
                    nextWriteBatch = null;
                    break;
                } else {
                    writeBatchDisruptor.getRingBuffer().publish(sequenceId);
                    nextWriteBatch = null;
                }
            }
        }
        return writeRecord.getLocation();
    }

    public void open() {
        opened = true;
        writeBatchDisruptor.start();
        writeCommandDisruptor.start();
    }

    public void close() throws IOException {
        sync();
        writeBatchDisruptor.shutdown();
        journal.setLastAppendLocation(null);
        if (lastAppendRaf != null) {
            lastAppendRaf.close();
        }
        writeCommandDisruptor.shutdown();
        opened = false;
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
                        journal.getInflightWrites().remove(current.getLocation());
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
