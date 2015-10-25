package org.apache.hyracks.data.std.api;

public interface IMath<T> {

    public T and(Number mask);

    public T shiftRight(Number positions);

    public T shiftLeft(Number positions);

    public T add(Number summand);

    public T add(T pointableSummand);

    public T sub(Number subtrahend);

    public T sub(T pointableSubtrahend);

    public T div(Number subtrahend);

    public T div(T pointableSubtrahend);

    public T mult(Number subtrahend);

    public T mult(T pointableSubtrahend);
}
