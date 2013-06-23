package io.reign.coord;

/**
 * 
 * @author ypai
 * 
 */
public interface PermitPoolSize {

    public int get();

    public <T extends PermitPoolSize> T initialize();
}
