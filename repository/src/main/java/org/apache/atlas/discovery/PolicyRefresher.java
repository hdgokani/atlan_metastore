package org.apache.atlas.discovery;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class PolicyRefresher {

    long delay = 12 * 1000; // delay in milliseconds
    LoopTask task = new LoopTask();
    Timer timer;

    public void start() {
        if (timer != null) {
            timer.cancel();
        }
        timer = new Timer("refreshPolicies");
        Date executionDate = new Date();
        timer.scheduleAtFixedRate(task, executionDate, delay);
    }

    private class LoopTask extends TimerTask {
        public void run() {
            if (AtlasAuthorization.getInstance() != null) {
                AtlasAuthorization.getInstance().refreshPolicies();
            }
        }
    }

}