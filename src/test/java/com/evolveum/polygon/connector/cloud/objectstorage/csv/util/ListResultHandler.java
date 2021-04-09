package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ResultsHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class ListResultHandler implements ResultsHandler {

    private List<ConnectorObject> objects = new ArrayList<>();

    @Override
    public boolean handle(ConnectorObject object) {
        objects.add(object);

        return true;
    }

    public List<ConnectorObject> getObjects() {
        return objects;
    }
}
