package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * Created by lazyman on 02/02/2017.
 */
public class Column {

    private final String name;
    private final int index;

    public Column(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Column column = (Column) o;

        if (index != column.index) return false;
        return Objects.equals(name, column.name);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{name, index});
    }

    @Override
    public String toString() {
        return "Column{n='" + name + "', i=" + index + '}';
    }
}
