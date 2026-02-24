package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import org.janelia.saalfeldlab.n5.IoPolicy;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public interface GcsIoPolicy extends IoPolicy {

    static boolean validBounds(long channelSize, long offset, long length) {

        if (offset < 0)
            return false;
        else if (channelSize > 0 && offset >= channelSize) // offset == 0 and arrayLength == 0 is okay
            return false;
        else if (length >= 0 && offset + length > channelSize)
            return false;

        return true;
    }

    class Unsafe implements GcsIoPolicy {

        protected final Storage storage;
        protected final String bucketName;

        public Unsafe(Storage storage, String bucketName) {
            this.storage = storage;
            this.bucketName = bucketName;
        }

        @Override
        public void write(String key, ReadData readData) throws IOException {

            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, key).build();
            try (OutputStream outputStream = Channels.newOutputStream(storage.writer(blobInfo))) {
                readData.writeTo(outputStream);
            }
        }

        @Override
        public VolatileReadData read(String key) {

            return VolatileReadData.from(new GCSLazyRead(storage, bucketName, key, false));
        }

        @Override
        public void delete(String key) {
            if (!key.endsWith("/")) {
                storage.delete(BlobId.of(bucketName, key));
            }


            /*
             * TODO consider instead using Object Lifecycle Management when deleting many items see:
             * https://cloud.google.com/storage/docs/deleting-objects#delete-objects-in-bulk
             */
            Page<Blob> page = storage.list(
                    bucketName,
                    Storage.BlobListOption.prefix(key),
                    Storage.BlobListOption.fields(Storage.BlobField.ID));

            while (page != null) {
                final BlobId[] ids = page.streamValues().map(Blob::getBlobId).toArray(BlobId[]::new);
                if (ids.length > 0) // storage throws an error if ids is empty
                    storage.delete(ids);
                page = page.getNextPage();
            }
        }
    }

    class GenerationMatch extends Unsafe {

        public GenerationMatch(Storage storage, String bucketName) {
            super(storage, bucketName);
        }

        @Override
        public VolatileReadData read(String key) {
            return VolatileReadData.from(new GCSLazyRead(storage, bucketName, key, true));
        }
    }

    class GCSLazyRead implements LazyRead {

        private final Storage storage;
        private final String bucketName;
        private final String normalKey;
        private final boolean generationMatch;
        private Long generation = null;


        GCSLazyRead(
                final Storage storage,
                final String bucketName,
                final String normalKey,
                final boolean generationMatch) {
            this.storage = storage;
            this.bucketName = bucketName;
            this.normalKey = normalKey;
            this.generationMatch = generationMatch;
        }

        private Blob getBlob(String normalKey, Storage.BlobGetOption... options) {
            final Blob blob;
            try {
                if (generationMatch && generation != null) {
                    final Storage.BlobGetOption[] generationMatchOptions = new Storage.BlobGetOption[options.length + 1];
                    System.arraycopy(options, 0, generationMatchOptions, 0, options.length);
                    generationMatchOptions[options.length] = Storage.BlobGetOption.generationMatch(generation);
                    BlobId blobId = BlobId.of(bucketName, normalKey);
                    blob = storage.get(blobId, generationMatchOptions);
                } else {
                    BlobId blobId = BlobId.of(bucketName, normalKey);
                    blob = storage.get(blobId, options);
                }
            } catch (StorageException e) {
                if (e.getCode() == 404)
                    throw new N5Exception.N5NoSuchKeyException("No such key. bucket: " + bucketName + ". key: " + normalKey);
                if (e.getCode() == 412)
                    throw new N5Exception.N5ConcurrentModificationException("Generation mismatch. bucket: " + bucketName + ". key: " + normalKey);
                throw e;
            }

            if (blob == null)
                throw new N5Exception.N5NoSuchKeyException("No such key. bucket: " + bucketName + ". key: " + normalKey);

            if (generationMatch && generation == null)
                generation = blob.getGeneration();

            return blob;
        }

        @Override
        public long size() {

            final Blob blob = getBlob(normalKey, Storage.BlobGetOption.fields(Storage.BlobField.SIZE, Storage.BlobField.GENERATION));
            return blob.getSize();
        }

        @Override
        public ReadData materialize(final long offset, final long length) {

            if (length > Integer.MAX_VALUE)
                throw new N5Exception.N5IOException("Attempt to materialize too large data");

            final Blob blob = getBlob(normalKey);
            try (ReadChannel from = blob.reader()) {

                final long channelSize = blob.getSize();
                if (!validBounds(channelSize, offset, length))
                    throw new IndexOutOfBoundsException();

                from.seek(offset);
                if (length > 0)
                    from.limit(offset + length);

                long readLength;
                if (length < 0)
                    readLength = channelSize;
                else
                    readLength = length;

                final ByteBuffer buf = ByteBuffer.allocate((int) readLength);
                from.read(buf);
                return ReadData.from(buf);

            } catch (IOException e) {
                throw new N5Exception.N5IOException(e);
            }
        }

        @Override
        public void close() {
            generation = null;
        }
    }
}
