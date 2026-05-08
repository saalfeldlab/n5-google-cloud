package org.janelia.saalfeldlab.n5.googlecloud.backend;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;

import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudUtils;

public class BackendGoogleCloudStorageFactory {

    private static Storage storage;

    public static Storage getOrCreateStorage() {

        if (storage == null) {

            // query a list of user's projects first
            final ResourceManager resourceManager = new GoogleCloudResourceManagerClient().create();

            final Iterator<Project> projectsIterator = resourceManager.list().iterateAll().iterator();
            if (!projectsIterator.hasNext())
                fail("No projects were found. Create a google cloud project first");

            // get first project id to run tests
            final String projectId = projectsIterator.next().getProjectId();

            storage = GoogleCloudUtils.createGoogleCloudStorage(projectId);
        }

        return storage;
    }
}
