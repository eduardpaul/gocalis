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

        // Persistent listener: reflect intercom lifecycle events raised by the
        // engine (including calls started/stopped elsewhere, e.g. the web UI or a
        // configured timeout) onto this node's status and second output. A call is
        // considered "ours" when any of its participants is one we are configured
        // for; with no configured participants we reflect every call.
        const eventHandler = (message) => {
            if (message.event !== "intercom_started" && message.event !== "intercom_ended") {
                return;
            }

            const participants = message.node_ids || [];
            // Intercom page microphones talking in the call (e.g. "web-1").
            const remotes = message.remote_ids || [];
            const configured = configuredIds(null);
            const relevant = configured.length === 0 ||
                participants.some((p) => configured.indexOf(p) !== -1);
            if (!relevant) {
                return;
            }

            const who = participants.concat(remotes.length ? ["intercom page"] : []).join(" ↔ ") || "call";
            if (message.event === "intercom_started") {
                node.status({ fill: "green", shape: "dot", text: `in call: ${who}` });
                return;
            }

            // intercom_ended
            const reason = message.reason || "ended";
            node.status({ fill: message.status === "error" ? "red" : "grey", shape: "ring", text: `ended (${reason})` });
            node.send([null, {
                topic: "gocalis/intercom",
                payload: {
                    event: "intercom_ended",
                    node_ids: participants,
                    remote_ids: remotes,
                    node_id: message.node_id,
                    peer_node_id: message.peer_node_id,
                    status: message.status,
                    message: message.message,
                    reason: reason,
                    duration_seconds: message.duration_seconds
                },
                node_id: message.node_id,
                node_ids: participants,
                remote_ids: remotes,
                reason: reason,
                duration_seconds: message.duration_seconds
            }]);
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

            // Determine the operation: start (default) or stop. A stop is requested
            // via msg.action === "intercom_stop"/"stop", msg.stop truthy, or
            // msg.payload === "stop".
            const wantsStop = msg.action === "intercom_stop" ||
                msg.action === "stop" ||
                msg.stop === true ||
                (typeof msg.payload === "string" && msg.payload.toLowerCase() === "stop");

            if (wantsStop) {
                // A stop targets a single participant; the engine ends the whole
                // call that node participates in.
                let stopNode = "";
                if (msg.node_id !== undefined) {
                    stopNode = String(msg.node_id);
                } else {
                    let configured = [];
                    try {
                        configured = configuredIds(msg);
                    } catch (err) {
                        node.status({ fill: "red", shape: "ring", text: "error evaluating nodes" });
                        done(`Failed to evaluate participant list: ${err.message}`);
                        return;
                    }
                    if (configured.length > 0) {
                        stopNode = configured[0];
                    }
                }
                if (!stopNode) {
                    node.status({ fill: "red", shape: "ring", text: "no node to stop" });
                    done("intercom stop requires a node_id (via msg.node_id or the configured participants)");
                    return;
                }

                node.status({ fill: "yellow", shape: "dot", text: `stopping ${stopNode}` });
                const ok = node.server.sendWS({ action: "intercom_stop", node_id: stopNode });
                if (!ok) {
                    node.status({ fill: "red", shape: "ring", text: "not connected" });
                    done("WebSocket not connected");
                    return;
                }

                msg.payload = { action: "intercom_stop", node_id: stopNode, status: "stopping" };
                msg.status = "stopping";
                send([msg, null]);
                done();
                return;
            }

            // Start: resolve the participant list. Priority: msg.node_ids array,
            // then msg.node_id + msg.peer_node_id, then the configured list. A lone
            // msg.node_id (e.g. carried over from an upstream wake trigger) does
            // not override the configured participants.
            let nodes = [];
            if (msg.node_ids !== undefined) {
                nodes = parseNodeIds(msg.node_ids);
            } else if (msg.peer_node_id !== undefined) {
                if (msg.node_id !== undefined) nodes.push(String(msg.node_id));
                nodes.push(String(msg.peer_node_id));
            } else if (node.nodeIds && String(node.nodeIds).trim() !== "") {
                try {
                    nodes = configuredIds(msg);
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating nodes" });
                    done(`Failed to evaluate participant list: ${err.message}`);
                    return;
                }
            }

            if (nodes.length < 2) {
                node.status({ fill: "yellow", shape: "ring", text: "need 2+ nodes" });
                done("Intercom requires at least two participant node IDs (via configuration, msg.node_ids, or msg.node_id + msg.peer_node_id)");
                return;
            }

            const duration = msg.duration_seconds !== undefined
                ? parseFloat(msg.duration_seconds)
                : node.duration;

            const payload = { action: "intercom", node_ids: nodes };
            if (duration && duration > 0) {
                payload.duration_seconds = duration;
            }

            node.status({ fill: "blue", shape: "dot", text: `calling ${nodes.join(" ↔ ")}` });

            try {
                const result = await node.server.request(payload, {
                    expectEvents: ["intercom_started"],
                    nodeId: nodes[0],
                    timeoutMs: 30000
                });

                if (result.event === "error" || result.status === "error") {
                    node.status({ fill: "red", shape: "ring", text: result.message || "error" });
                    done(`Gocalis engine reported failure: ${result.message || "error"}`);
                    return;
                }

                msg.payload = result;
                msg.status = "started";
                msg.node_ids = result.node_ids || nodes;
                node.status({ fill: "green", shape: "dot", text: `in call: ${nodes.join(" ↔ ")}` });
                send([msg, null]);
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
