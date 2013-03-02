package org.kompany.overlord.coord;

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
    public int get() {
        return size;
    }

}
