package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.IoPolicy;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public interface GcsIoPolicy extends IoPolicy {

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

            return VolatileReadData.from(new GoogleCloudStorageKeyValueAccess.GCSLazyRead(storage, bucketName, key, false));
        }

        @Override
        public void delete(String key) {
        }
    }

    class GenerationMatch extends Unsafe {

        public GenerationMatch(Storage storage, String bucketName) {
            super(storage, bucketName);
        }

        @Override
        public VolatileReadData read(String key) {
            return VolatileReadData.from(new GoogleCloudStorageKeyValueAccess.GCSLazyRead(storage, bucketName, key, true));
        }
    }
}
