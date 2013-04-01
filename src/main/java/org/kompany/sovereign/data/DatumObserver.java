package org.kompany.sovereign.data;

public interface DatumObserver<T> {

    public void updated(T data);
}
