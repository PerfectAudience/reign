package org.kompany.overlord.coord;

import java.util.Comparator;

/**
 * 
 * @author ypai
 * 
 */
public class LockReservationComparator implements Comparator<String> {

    private final String delimiter;

    public LockReservationComparator(String delimiter) {
        super();
        this.delimiter = delimiter;
    }

    @Override
    public int compare(String arg0, String arg1) {
        if (arg0 == null && arg1 != null) {
            return -1;
        }
        if (arg0 != null && arg1 == null) {
            return 1;
        }

        long seq0 = parseSequenceNumber(arg0, delimiter);
        long seq1 = parseSequenceNumber(arg1, delimiter);

        if (seq0 > seq1) {
            return 1;
        }
        if (seq0 < seq1) {
            return -1;
        }

        return 0;
    }

    long parseSequenceNumber(String nodePath, String delimiter) {
        try {
            int delimiterIndex = nodePath.lastIndexOf(delimiter);
            return Long.parseLong(nodePath.substring(delimiterIndex + 1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Could parse sequence number:  " + e, e);
        }
    }
}
