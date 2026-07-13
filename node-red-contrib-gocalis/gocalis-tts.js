module.exports = function(RED) {
    function getValueFromPath(obj, path) {
        if (!path) return undefined;
        if (path.startsWith('msg.')) {
            path = path.slice(4);
        }
        const parts = path.split('.');
        let current = obj;
        for (const part of parts) {
            if (current === null || current === undefined) {
                return undefined;
            }
            current = current[part];
        }
        return current;
    }

    function interpolate(text, msg) {
        if (typeof text !== 'string') return '';
        return text.replace(/\{\{([^}]+)\}\}|\{([^}]+)\}/g, (match, p1, p2) => {
            const path = (p1 || p2).trim();
            const val = getValueFromPath(msg, path);
            return val !== undefined ? (typeof val === 'object' ? JSON.stringify(val) : String(val)) : match;
        });
    }

    function GocalisTtsNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "default";
        this.nodeIdType = config.nodeIdType || "str";
        this.text = config.text;
        this.textType = config.textType || "str";
        this.priority = parseInt(config.priority) || 0;

        const node = this;

        node.on('input', async function(msg, send, done) {
            send = send || function() { node.send.apply(node, arguments); };
            done = done || function(err) { if (err) { node.error(err, msg); } };

            if (!node.server) {
                node.status({ fill: "red", shape: "ring", text: "missing config" });
                done("Missing Gocalis configuration");
                return;
            }

            let textToSynthesize = "";
            if (node.text && node.text.trim() !== "") {
                try {
                    const rawVal = RED.util.evaluateNodeProperty(node.text, node.textType, node, msg);
                    if (typeof rawVal === 'string') {
                        textToSynthesize = interpolate(rawVal, msg);
                    } else if (rawVal !== undefined && rawVal !== null) {
                        textToSynthesize = typeof rawVal === 'object' ? JSON.stringify(rawVal) : String(rawVal);
                    }
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating property" });
                    done(`Failed to evaluate text property: ${err.message}`);
                    return;
                }
            } else {
                textToSynthesize = msg.payload;
            }

            if (!textToSynthesize || typeof textToSynthesize !== 'string') {
                node.status({ fill: "yellow", shape: "ring", text: "invalid payload" });
                done("Payload must be a string containing the text to synthesize.");
                return;
            }

            let resolvedNodeId = "default";
            if (node.nodeId && node.nodeId.trim() !== "") {
                try {
                    resolvedNodeId = RED.util.evaluateNodeProperty(node.nodeId, node.nodeIdType, node, msg);
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating node ID" });
                    done(`Failed to evaluate node ID property: ${err.message}`);
                    return;
                }
            }
            if (msg.node_id !== undefined) {
                resolvedNodeId = String(msg.node_id);
            } else if (msg.nodeId !== undefined) {
                resolvedNodeId = String(msg.nodeId);
            } else if (msg.room !== undefined) {
                resolvedNodeId = String(msg.room);
            }

            node.status({ fill: "blue", shape: "dot", text: "speaking..." });

            const payload = {
                action: "tts",
                node_id: resolvedNodeId,
                text: textToSynthesize,
                priority: msg.priority !== undefined ? parseInt(msg.priority) : node.priority
            };

            try {
                const result = await node.server.request(payload, {
                    expectEvents: ["tts_completed"],
                    nodeId: resolvedNodeId
                });

                msg.payload = result;
                msg.status = result.status;
                msg.node_id = result.node_id || resolvedNodeId;

                if (result.event === "error" || result.status === "error") {
                    node.status({ fill: "red", shape: "ring", text: result.message || "error" });
                    done(`Gocalis engine reported failure: ${result.message || "error"}`);
                    return;
                }

                node.status({ fill: "green", shape: "dot", text: "done" });
                send(msg);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "request failed" });
                done(`WebSocket request failed: ${err.message}`);
            }
        });
    }

    RED.nodes.registerType("gocalis-tts", GocalisTtsNode);
}
