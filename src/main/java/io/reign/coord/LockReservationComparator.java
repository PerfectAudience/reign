/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package io.reign.coord;

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
