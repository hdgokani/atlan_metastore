package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.repository.graph.IndexChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class IndexChangeListenerManager {

    private static final Logger LOG = LoggerFactory.getLogger(IndexChangeListenerManager.class);


    private final List<IndexChangeListener> indexChangeListeners = new ArrayList<>();

    public void addIndexListener(IndexChangeListener listener) {
        indexChangeListeners.add(listener);
    }

    public void notifyChangeListeners(ChangedTypeDefs changedTypeDefs) {
        for (IndexChangeListener indexChangeListener : indexChangeListeners) {
            try {
                indexChangeListener.onChange(changedTypeDefs);
            } catch (Throwable t) {
                LOG.error("Error encountered in notifying the index change listener {}.", indexChangeListener.getClass().getName(), t);
                //we need to throw exception if any of the listeners throw execption.
                throw new RuntimeException("Error encountered in notifying the index change listener " + indexChangeListener.getClass().getName(), t);
            }
        }
    }

    public void notifyInitializationStart() {
        for (IndexChangeListener indexChangeListener : indexChangeListeners) {
            try {
                indexChangeListener.onInitStart();
            } catch (Throwable t) {
                LOG.error("Error encountered in notifying the index change listener {}.", indexChangeListener.getClass().getName(), t);
                //we need to throw exception if any of the listeners throw execption.
                throw new RuntimeException("Error encountered in notifying the index change listener " + indexChangeListener.getClass().getName(), t);
            }
        }
    }

    public void notifyInitializationCompletion(ChangedTypeDefs changedTypeDefs) {
        for (IndexChangeListener indexChangeListener : indexChangeListeners) {
            try {
                indexChangeListener.onInitCompletion(changedTypeDefs);
            } catch (Throwable t) {
                LOG.error("Error encountered in notifying the index change listener {}.", indexChangeListener.getClass().getName(), t);
                //we need to throw exception if any of the listeners throw execption.
                throw new RuntimeException("Error encountered in notifying the index change listener " + indexChangeListener.getClass().getName(), t);
            }
        }
    }
}
