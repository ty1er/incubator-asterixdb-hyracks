package org.apache.hyracks.util.objectpool;

import java.util.HashMap;
import java.util.Map;

public class MapObjectPool<E, T> implements IObjectPool<E, T> {

    private final Map<T, IObjectFactory<E, T>> factoryMap;
    private final Map<T, ObjectCache<E>> cacheMap;

    public MapObjectPool() {
        factoryMap = new HashMap<T, IObjectFactory<E, T>>();
        cacheMap = new HashMap<T, ObjectCache<E>>();
    }

    public void register(T arg, IObjectFactory<E, T> factory) {
        factoryMap.put(arg, factory);
        cacheMap.put(arg, new ObjectCache<E>());
    }

    @Override
    public E allocate(T arg) {
        ObjectCache<E> cache = cacheMap.get(arg);
        E newObj = cache.takeOne();
        if (newObj != null) {
            return newObj;
        }
        IObjectFactory<E, T> factory = factoryMap.get(arg);
        return factory.create(arg);
    }

    public void deallocate(T arg, E obj) {
        ObjectCache<E> pc = cacheMap.get(arg);
        pc.giveBack(obj);
    }

    @Override
    public void reset() {
        factoryMap.clear();
        factoryMap.clear();
    }

}
