package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import java.util.Arrays;

/**
 * Created by lazyman on 02/02/2017.
 */
public class Column {

    private String name;
    private int index;

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
        return name != null ? name.equals(column.name) : column.name == null;
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
