module.exports = function(RED) {
    const WebSocket = require('ws');

    function GocalisConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.host = n.host || "localhost";
        this.port = n.port || "9090";
        this.ssl = n.ssl || false;

        this.apiKey = n.apiKey !== undefined ? n.apiKey : "demo_password";

        const node = this;

        // Determine protocols
        const wsProto = node.ssl ? "wss" : "ws";
        const httpProto = node.ssl ? "https" : "http";

        node.baseUrl = `${httpProto}://${node.host}:${node.port}`;
        if (node.apiKey) {
            node.wsUrl = `${wsProto}://${node.host}:${node.port}/ws?token=${encodeURIComponent(node.apiKey)}`;
        } else {
            node.wsUrl = `${wsProto}://${node.host}:${node.port}/ws`;
        }

        node.ws = null;
        node.closing = false;

        // Pending WS requests awaiting a correlated completion event.
        // Each entry: { match(fn), resolve, timer }.
        node.pending = [];

        // Resolve the oldest pending request whose matcher accepts this message.
        node._dispatch = function(message) {
            for (let i = 0; i < node.pending.length; i++) {
                const p = node.pending[i];
                if (p.match(message)) {
                    clearTimeout(p.timer);
                    node.pending.splice(i, 1);
                    p.resolve(message);
                    return;
                }
            }
        };

        function connectWS() {
            if (node.closing) return;
            node.log(`Connecting to Gocalis WebSocket: ${node.wsUrl}`);
            node.ws = new WebSocket(node.wsUrl);

            node.ws.on('open', () => {
                node.log("Connected to Gocalis WebSocket server.");
            });

            node.ws.on('message', (data) => {
                try {
                    const message = JSON.parse(data.toString());
                    node.emit('gocalis-event', message);
                    node._dispatch(message);
                } catch (e) {
                    node.error("Failed to parse Gocalis WebSocket message: " + e.message);
                }
            });

            node.ws.on('close', () => {
                node.ws = null;
                if (!node.closing) {
                    node.warn("Gocalis WebSocket connection closed. Reconnecting in 5 seconds...");
                    setTimeout(connectWS, 5000);
                }
            });

            node.ws.on('error', (err) => {
                node.error("Gocalis WebSocket error: " + err.message);
            });
        }

        connectWS();

        // Helper to send messages over WebSocket (e.g. for cancellation)
        node.sendWS = function(payload) {
            if (node.ws && node.ws.readyState === WebSocket.OPEN) {
                node.ws.send(JSON.stringify(payload));
                return true;
            }
            return false;
        };

        // Send an action Request over the WebSocket and resolve with the
        // correlated completion event. Correlation is by (event name, node_id):
        // the returned Promise resolves on the first event whose `event` is in
        // opts.expectEvents (or is "error") and whose node_id matches opts.nodeId.
        // The synchronous "{action}_accepted" ack is ignored. Rejects on timeout
        // or if the socket is not open.
        node.request = function(payload, opts) {
            opts = opts || {};
            const expect = opts.expectEvents || [];
            const nodeId = opts.nodeId;
            const timeoutMs = opts.timeoutMs || 30000;

            return new Promise((resolve, reject) => {
                if (!node.ws || node.ws.readyState !== WebSocket.OPEN) {
                    reject(new Error("WebSocket not connected"));
                    return;
                }

                const match = (m) => {
                    if (nodeId !== undefined && nodeId !== null && m.node_id !== nodeId) {
                        return false;
                    }
                    if (m.event === 'error') return true;
                    return expect.indexOf(m.event) !== -1;
                };

                const entry = { match, resolve, timer: null };
                entry.timer = setTimeout(() => {
                    const idx = node.pending.indexOf(entry);
                    if (idx !== -1) node.pending.splice(idx, 1);
                    reject(new Error("timeout waiting for response"));
                }, timeoutMs);
                node.pending.push(entry);

                try {
                    node.ws.send(JSON.stringify(payload));
                } catch (e) {
                    clearTimeout(entry.timer);
                    const idx = node.pending.indexOf(entry);
                    if (idx !== -1) node.pending.splice(idx, 1);
                    reject(e);
                }
            });
        };

        node.on('close', function(done) {
            node.closing = true;
            if (node.ws) {
                node.ws.close();
            }
            done();
        });
    }

    RED.nodes.registerType("gocalis-config", GocalisConfigNode);
}
