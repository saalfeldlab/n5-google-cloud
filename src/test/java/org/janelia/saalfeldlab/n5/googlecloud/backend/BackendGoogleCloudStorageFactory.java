/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.googlecloud.backend;

import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;

import java.util.Iterator;

import static org.junit.Assert.fail;

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

            storage = new GoogleCloudStorageClient(projectId).create();
        }

        return storage;
    }
}
