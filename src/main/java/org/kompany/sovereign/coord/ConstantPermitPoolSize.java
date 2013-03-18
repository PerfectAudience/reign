package org.kompany.sovereign.coord;

/**
 * 
 * @author ypai
 * 
 */
public class ConstantPermitPoolSize implements PermitPoolSize {

    private final int size;

    public ConstantPermitPoolSize(int size) {
        this.size = size;
    }

    @Override
    public ConstantPermitPoolSize initialize() {
        return this;
    }

    @Override
    public int get() {
        return size;
    }

}
