package org.apache.hyracks.util.objectpool;

import java.util.ArrayList;
import java.util.List;

public class ObjectCache<T> {
    private final List<T> list;

    public ObjectCache() {
        list = new ArrayList<T>();
    }

    public T takeOne() {
        if (list.isEmpty()) {
            return null;
        }
        return list.remove(list.size() - 1);
    }

    public void giveBack(T obj) {
        list.add(obj);
    }

}
