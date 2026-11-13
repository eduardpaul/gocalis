module.exports = function(RED) {
    // Parse a participant list from a string ("kitchen, bedroom") or an array
    // into a clean array of non-empty node IDs.
    function parseNodeIds(value) {
        if (Array.isArray(value)) {
            return value.map((v) => String(v).trim()).filter((v) => v !== "");
        }
        if (typeof value === "string") {
            return value.split(",").map((v) => v.trim()).filter((v) => v !== "");
        }
        return [];
    }

    function GocalisIntercomNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeIds = config.nodeIds || "";
        this.nodeIdsType = config.nodeIdsType || "str";
        this.duration = parseFloat(config.duration) || 0;

        const node = this;

        // configuredIds resolves the configured participant list. A plain string
        // is parsed directly; other typed-input kinds (msg/flow/global/env/jsonata)
        // are evaluated against msg when one is available. Without a msg (e.g. in
        // the event listener) a non-string list cannot be resolved, so [] is
        // returned and every call is treated as relevant.
        function configuredIds(msg) {
            if (!node.nodeIds || String(node.nodeIds).trim() === "") {
                return [];
            }
            if (node.nodeIdsType === "str") {
                return parseNodeIds(node.nodeIds);
            }
            if (!msg) {
                return [];
            }
            return parseNodeIds(RED.util.evaluateNodeProperty(node.nodeIds, node.nodeIdsType, node, msg));
        }

        if (!node.server) {
            node.status({ fill: "red", shape: "ring", text: "missing config" });
        } else {
            node.status({ fill: "grey", shape: "ring", text: "idle" });
        }

        // Persistent listener: reflect what happens to calls (also the ones
        // started, joined or ended elsewhere: the intercom page, the dashboard,
        // a timeout) onto this node's status and its outputs 2 and 3. A call is
        // "ours" when it involves a node we are configured for; with no
        // configured participants we reflect every call.
        const eventHandler = (message) => {
            const kind = message.event;
            if (kind !== "intercom_ringing" && kind !== "intercom_started" && kind !== "intercom_joined" &&
                kind !== "intercom_left" && kind !== "intercom_ended") {
                return;
            }

            const nodes = message.node_ids || [];
            // Intercom pages in the call (e.g. "web-1").
            const remotes = message.remote_ids || [];
            const configured = configuredIds(null);
            const relevant = configured.length === 0 ||
                configured.indexOf(message.participant_id) !== -1 ||
                nodes.some((p) => configured.indexOf(p) !== -1);
            if (!relevant) {
                return;
            }

            const out = {
                topic: "gocalis/intercom",
                payload: message,
                event: kind,
                node_ids: nodes,
                remote_ids: remotes
            };

            if (kind === "intercom_ended") {
                const reason = message.reason || "ended";
                node.status({ fill: message.status === "error" ? "red" : "grey", shape: "ring", text: `ended (${reason})` });
                out.reason = reason;
                out.duration_seconds = message.duration_seconds;
                node.send([null, null, out]);
                return;
            }

            if (kind === "intercom_ringing") {
                // A node somebody has to answer is being rung: for a call that
                // starts when they do, or (participant_id) to join a running one.
                node.status({ fill: "blue", shape: "ring", text: `ringing ${message.participant_id || nodes.join(", ")}` });
                out.participant_id = message.participant_id;
                node.send([null, out, null]);
                return;
            }

            node.status({ fill: "green", shape: "dot", text: `in call: ${nodes.concat(remotes).join(", ")}` });
            if (kind !== "intercom_started") {
                out.participant_id = message.participant_id;
                out.reason = message.reason;
                node.send([null, out, null]);
            }
        };

        if (node.server) {
            node.server.on('gocalis-event', eventHandler);
        }

        node.on('input', async function(msg, send, done) {
            send = send || function() { node.send.apply(node, arguments); };
            done = done || function(err) { if (err) { node.error(err, msg); } };

            if (!node.server) {
                node.status({ fill: "red", shape: "ring", text: "missing config" });
                done("Missing Gocalis configuration");
                return;
            }

            // The operation: "connect" (default) puts the participants in a call
            // together; "leave" takes one node out of its call; "stop" ends the
            // whole call. It comes from msg.action, or from msg.payload when
            // that is the string "leave" or "stop".
            let action = "connect";
            const asked = typeof msg.action === "string" ? msg.action
                : (typeof msg.payload === "string" ? msg.payload : "");
            if (/^(intercom_)?(leave|stop)$/i.test(asked)) {
                action = asked.toLowerCase().replace("intercom_", "");
            }

            let configured;
            try {
                configured = configuredIds(msg);
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "error evaluating nodes" });
                done(`Failed to evaluate participant list: ${err.message}`);
                return;
            }

            if (action !== "connect") {
                // Leave and stop name one node: the one that leaves, or any
                // participant of the call to end.
                const target = msg.node_id !== undefined ? String(msg.node_id) : (configured[0] || "");
                if (!target) {
                    node.status({ fill: "red", shape: "ring", text: `no node to ${action}` });
                    done(`intercom ${action} requires a node (msg.node_id or the configured participants)`);
                    return;
                }
                if (!node.server.sendWS({ action: `intercom_${action}`, node_id: target })) {
                    node.status({ fill: "red", shape: "ring", text: "not connected" });
                    done("WebSocket not connected");
                    return;
                }
                msg.payload = { action: `intercom_${action}`, node_id: target };
                send([msg, null, null]);
                done();
                return;
            }

            // Connect: msg.node_ids overrides the configured participants.
            const nodes = msg.node_ids !== undefined ? parseNodeIds(msg.node_ids) : configured;
            if (nodes.length < 2) {
                node.status({ fill: "yellow", shape: "ring", text: "need 2+ nodes" });
                done("Intercom requires at least two participant node IDs (configured, or in msg.node_ids)");
                return;
            }

            const duration = msg.duration_seconds !== undefined
                ? parseFloat(msg.duration_seconds)
                : node.duration;

            const payload = { action: "intercom", node_ids: nodes };
            if (duration && duration > 0) {
                payload.duration_seconds = duration;
            }

            node.status({ fill: "blue", shape: "dot", text: `calling ${nodes.join(", ")}` });

            try {
                // The request is answered by the event that shows all our nodes
                // in one call (a new call, or the last of them joining a running
                // one), or by the error raised for it. A node somebody has to
                // answer is rung first, so that can take its ring time; when
                // nobody answers, the call ends without ever having started.
                const result = await node.server.request(payload, {
                    match: (m) => (m.event === "error" && m.node_id === nodes[0]) ||
                        ((m.event === "intercom_started" || m.event === "intercom_joined") &&
                            nodes.every((n) => (m.node_ids || []).indexOf(n) !== -1)) ||
                        (m.event === "intercom_ended" &&
                            nodes.some((n) => (m.node_ids || []).indexOf(n) !== -1)),
                    timeoutMs: 300000
                });

                if (result.event === "error") {
                    node.status({ fill: "red", shape: "ring", text: result.message || "error" });
                    done(`Gocalis engine reported failure: ${result.message || "error"}`);
                    return;
                }
                if (result.event === "intercom_ended") {
                    // Output 3 has the event; there is no call to report here.
                    done();
                    return;
                }

                msg.payload = result;
                msg.node_ids = result.node_ids;
                msg.remote_ids = result.remote_ids || [];
                send([msg, null, null]);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "request failed" });
                done(`WebSocket request failed: ${err.message}`);
            }
        });

        node.on('close', function() {
            if (node.server) {
                node.server.removeListener('gocalis-event', eventHandler);
            }
        });
    }

    RED.nodes.registerType("gocalis-intercom", GocalisIntercomNode);
}
