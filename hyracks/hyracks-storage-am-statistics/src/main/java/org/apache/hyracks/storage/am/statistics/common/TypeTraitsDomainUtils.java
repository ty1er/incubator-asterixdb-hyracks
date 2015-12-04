package org.apache.hyracks.storage.am.statistics.common;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;

// Class methods obtain numeric domain bounds by considering appropriate ITypeTraits assuming that numeric domain is signed
public class TypeTraitsDomainUtils {

    public static long minDomainValue(ITypeTraits typeTraits) throws HyracksDataException {
        return ~maxDomainValue(typeTraits);
    }

    public static long maxDomainValue(ITypeTraits typeTraits) throws HyracksDataException {
        if (!typeTraits.isFixedLength())
            throw new HyracksDataException("Cannot calculate domain for variable size type");
        // subtract 1 from fixedLength assuming that 1 byte is reserved for type tag
        return (1l << (((typeTraits.getFixedLength() - 1) * 8) - 1)) - 1;
    }

    public static int maxLevel(ITypeTraits typeTraits) throws HyracksDataException {
        if (!typeTraits.isFixedLength())
            throw new HyracksDataException("Cannot calculate domain for variable size type");
        // subtract 1 from fixedLength assuming that 1 byte is reserved for type tag
        return ((typeTraits.getFixedLength() - 1) * 8);
    }
}
