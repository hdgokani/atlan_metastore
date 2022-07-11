package org.apache.atlas.web.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionStateListenerService implements ConnectionStateListener {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveInstanceElectorService.class);

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        LOG.info("Connection State Changed!");
        LOG.info("Connection state changed to - {}", connectionState.toString());
    }
}
