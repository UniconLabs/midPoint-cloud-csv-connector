package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvFilterTranslator extends AbstractFilterTranslator<String> {

    @Override
    protected String createEqualsExpression(EqualsFilter filter, boolean not) {
        if (not) {
            return null;
        }

        Attribute attr = filter.getAttribute();
        if (!attr.is(Uid.NAME)) {
            return null;
        }

        List<Object> values = attr.getValue();
        if (values.isEmpty()) {
            return null;
        }

        Object value = values.get(0);

        return value != null ? value.toString() : null;
    }
}
