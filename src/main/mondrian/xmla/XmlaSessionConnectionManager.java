/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

import org.apache.log4j.Logger;

import org.olap4j.OlapConnection;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Services connections which may be reused based on sessionId.
 * SessionConnection objects are obtained with
 * {@link #getConnectionGrant} and must be returned via
 * {@link #releaseConnection(SessionConnection)}.<br>
 * (EXPERIMENTAL) 
 * TODO: doesn't take schema refresh into account;
 * no good for dynamic schemas that may change for the same session;
 */
public class XmlaSessionConnectionManager {

  private static final Logger LOGGER =
      Logger.getLogger(XmlaSessionConnectionManager.class);
    /**
     * Fired periodically to remove timed out sessions.
     */
    private static final ScheduledExecutorService executorService =
        Util.getScheduledExecutorService(
            1,
            "mondrian.xmla.XmlaSessionConnectionManager$executorService");

    /**
     * Margin on the TTL in ms. Prevents the executor
     * from being summoned too often.
     */
    private final static long timeoutDeltaMs = 300;
    private final long sessionTtlSeconds;
    private final int queueInitialCapacity = 600;
    private final int maxSessionConnections;
    private final int maxQueueSize;
    private final boolean enabled;

    /**
     * The one lock for the collections.
     */
    private final Object sessionsLock = new Object();
    /**
     * The sessionId-based pool.
     */
    private final Map<String, SessionConnection> sessionConnections =
        new HashMap<String, SessionConnection>();
    /**
     * Connections queue ordered by sooner expiration date.
     * Objects may only be closed when pulled from the Map, not the queue. They
     * may still exist in the queue without being in the map but not the other
     * way around.
     * TODO: ditch queue and just use a single LinkedHashMap instead.
     */
    private final Queue<SessionConnection> timeoutQueue =
        new PriorityQueue<SessionConnection>(queueInitialCapacity,
            new Comparator<SessionConnection>() {
              public int compare(SessionConnection o1, SessionConnection o2) {
                  return o1.timeOfDeath < o2.timeOfDeath
                      ? -1
                      : o1.timeOfDeath > o2.timeOfDeath
                          ? 1
                          : 0;
              }
        });
    private XmlaHandler xmlaHandler;
    private boolean cleanerRunning = false;

    public XmlaSessionConnectionManager(XmlaHandler parent)
    {
        this.xmlaHandler = parent;
        this.enabled =
            MondrianProperties.instance().XmlaSessionConnectionManagement.get();
        this.sessionTtlSeconds =
            MondrianProperties.instance().XmlaSessionConnectionTTLSeconds.get();
        this.maxSessionConnections =
            MondrianProperties.instance().XmlaMaxSessionConnections.get();
        this.maxQueueSize =
            MondrianProperties.instance().XmlaMaxSCCleanQueueSize.get();
    }

    /**
     * Create a new connection or reuse if available.
     * @param request
     * @param properties
     * @return
     */
    public SessionConnection getConnectionGrant(
        XmlaRequest request,
        Map<String, String> properties)
    {
        String sessionId = request.getSessionId();
        long timeToDie = 0L;
        if (enabled && sessionId != null) {

            synchronized (sessionsLock) {
                SessionConnection sc = sessionConnections.remove(sessionId);
                if (sc != null && !isStale(sc)) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(
                            "Reusing connection for sessionId=" + sessionId);
                    }
                    return sc;
                }
            }

            // else just create a new one
            timeToDie = System.currentTimeMillis()
                + (sessionTtlSeconds * 1000);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("New connection for sessionId=" + sessionId);
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                "post-lock, New connection for sessionId=" + sessionId);
        }
        return new SessionConnection(
            request.getSessionId(),
            xmlaHandler.getConnection(request, properties),
            timeToDie);
    }

    /**
     * Closes the connection or keeps it for reuse
     */
    public void releaseConnection(SessionConnection sc) {
        if (!enabled) {
            sc.closeQuietly();
            return;
        }
        if (!isStale(sc) && sc.sessionId != null) {
            synchronized (sessionsLock) {
                saveConnection(sc);
            }
        } else {
            sc.closeQuietly();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Closed connection for sessionId=" + sc.sessionId);
            }
        }
    }

    public void endSession(String sessionId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Session ended sessionId=" + sessionId);
        }
        if (!enabled) {
            return;
        }
        synchronized (sessionsLock) {
            removeConnection(sessionId, true);
        }
    }

    /**
     * Close all saved connections.
     */
    public void close() {
        if (!enabled) {
            return;
        }
        synchronized (sessionsLock) {
            evictAll();
            executorService.shutdownNow();
        }
    }

    /**
     * (MUST SYNC)
     */
    private void evictAll() {
        for (SessionConnection sc : sessionConnections.values()) {
          sc.closeQuietly();
        }
        sessionConnections.clear();
        timeoutQueue.clear();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("All connections closed and evicted.");
        }
    }

    /**
     * (MUST SYNC)
     * @param sc 
     */
    private void saveConnection(SessionConnection sc) {
        SessionConnection old =
            sessionConnections.put(sc.sessionId, sc);
        if (old != null) {
            // this can happen if we get concurrent xmla requests
            // with the same session; just close and move on
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Clash for sessionId=" + old.sessionId);
            }
            if (old.timeOfDeath > sc.timeOfDeath) {
                // keep "old" instead
                sc.closeQuietly();
                sessionConnections.put(old.sessionId, old);
                return;
            }
            else {
                // remove old and keeping adding new
                old.closeQuietly();
                timeoutQueue.remove(old);
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                "Saving connection for sessionId=" + sc.sessionId);
        }
        addToQueue(sc);
    }

    private static boolean isStale(SessionConnection sc) {
        return sc.timeOfDeath <= (System.currentTimeMillis() + timeoutDeltaMs);
    }

    /**
     * (MUST SYNC)
     * @param sc the connection
     */
    private void addToQueue(SessionConnection sc) {
        if (sc == timeoutQueue.peek()) {
            // avoid consecutive adds
            return;
        }
        timeoutQueue.add(sc);
        if (sessionConnections.size() > maxSessionConnections || 
            timeoutQueue.size() > maxQueueSize ) {
            // just panic
            evictAll();
        }
        if (!cleanerRunning) {
            reschedule(timeoutQueue.peek());
            cleanerRunning = true;
        }
    }

    /**
     * (MUST SYNC)
     * @param sc the connection
     */
    private void reschedule(SessionConnection sc) {
        long interval = sc.timeOfDeath - System.currentTimeMillis();
        executorService.schedule(
            new ConnectionTimeoutInspector(),
            interval,
            TimeUnit.MILLISECONDS);
    }

    /**
     * Cleans stale elements from queue and reschedules itself for
     * next remaining item.
     */
    private class ConnectionTimeoutInspector implements Runnable {
        public void run() {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Cleaner running");
            }
            synchronized (sessionsLock) {
                // we don't have to close anything here;
                // if it's not in the map it should be closed
                SessionConnection sc = timeoutQueue.peek();
                int count = 0;
                while (sc != null && isStale(sc)) {
                    count++;
                    timeoutQueue.remove();
                    removeConnection(sc.sessionId, false);
                    sc = timeoutQueue.peek();
                }
                if (LOGGER.isDebugEnabled()) {
                    String en = (sc == null) ? " (empty)" : "";
                    LOGGER.debug("Cleaned " + count + en);
                }
                if (sc == null) {
                    // empty queue
                    cleanerRunning = false;
                } else {
                    // reschedule for next
                    reschedule(sc);
                }
            }
        }
    }

    /**
     * (MUST SYNC)
     * removes if stale
     * @param sessionId
     * @param removeFromQueue
     */
    private void removeConnection(String sessionId, boolean removeFromQueue) {
        assert sessionId != null;
        SessionConnection sc = sessionConnections.remove(sessionId);
        if (sc != null) {
            if (!isStale(sc)) {
                sessionConnections.put(sessionId, sc);
                LOGGER.warn(
                    "Trying to remove fresh connection for sessionId="
                    + sessionId);
            } else {
                sc.closeQuietly();
                if (removeFromQueue) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                            "Remove from queue: sessionId="
                            + sessionId);
                    }
                    timeoutQueue.remove(sc);
                }
            }
        }
    }

    /**
     * A wrapper for an OlapConnection. Also keeps to track of expiry date and
     * associated session (if any)
     */
    public static final class SessionConnection {
        private String sessionId;
        private OlapConnection connection;
        /**
         * Expiration time in ms
         */
        private long timeOfDeath;

        private SessionConnection(
            String sessionId,
            OlapConnection connection,
            long timeOfDeath)
        {
            assert connection != null;
            this.connection = connection;
            this.sessionId = sessionId;
            this.timeOfDeath = timeOfDeath;
        }

        void closeQuietly() {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignored
                }
                connection = null;
            }
        }

        public OlapConnection getConnection() {
            return connection;
        }
    }
}
// End XmlaSessionConnectionManager.java