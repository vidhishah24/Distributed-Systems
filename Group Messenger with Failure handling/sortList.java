package edu.buffalo.cse.cse486586.groupmessenger2;

import java.util.ArrayList;

/**
 * Created by root on 3/26/17.
 */

public class sortList {
    public static String sortArrayList(ArrayList<String> msgList) {
        String maxString = "";
        String maxPort = "11107";

        for (int i = 0; i < msgList.size(); i++) {
            String msgListArray[] = msgList.get(i).split(":");
            if (Integer.parseInt(msgListArray[3]) > Integer.parseInt(maxPort)) {
                maxPort = msgListArray[3];
                maxString = msgList.get(i);
            }
        }
        return maxString;

    }
}
