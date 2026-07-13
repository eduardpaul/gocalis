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

    function GocalisSayNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "default";
        this.nodeIdType = config.nodeIdType || "str";
        this.text = config.text;
        this.textType = config.textType || "str";
        this.bargeIn = !!config.bargeIn;
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

            const hasExplicitText = !!(node.text && node.text.trim() !== "");

            let textToSay = "";
            if (hasExplicitText) {
                try {
                    const rawVal = RED.util.evaluateNodeProperty(node.text, node.textType, node, msg);
                    if (typeof rawVal === 'string') {
                        textToSay = interpolate(rawVal, msg);
                    } else if (rawVal !== undefined && rawVal !== null) {
                        textToSay = typeof rawVal === 'object' ? JSON.stringify(rawVal) : String(rawVal);
                    }
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating property" });
                    done(`Failed to evaluate text property: ${err.message}`);
                    return;
                }
            } else {
                textToSay = msg.payload;
            }

            let audioBase64 = msg.audio_base64 || msg.audio_wav_base64;
            // Only dig into msg.payload for audio when this node has NO explicit
            // text configured. A node meant to speak a fixed phrase (e.g. an
            // acknowledgement) must not instead replay an ask-result object that
            // happens to still be sitting in msg.payload — even after the caller
            // cleared the top-level audio_* fields. (msg.payload from a
            // gocalis-ask node is the whole result, with audio_wav_base64 nested.)
            if (!audioBase64 && !hasExplicitText && msg.payload && typeof msg.payload === 'object') {
                audioBase64 = msg.payload.audio_wav_base64;
            }

            if (!audioBase64) {
                if (!textToSay || typeof textToSay !== 'string') {
                    node.status({ fill: "yellow", shape: "ring", text: "invalid payload" });
                    done("Payload must be a string containing the text to speak, or msg.audio_wav_base64 must be provided.");
                    return;
                }
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

            if (audioBase64 && (!textToSay || typeof textToSay !== 'string' || textToSay.trim() === "")) {
                // The WS API only exposes text-to-speech; there is no action to
                // play back arbitrary recorded audio on a node. Fail clearly so
                // flows relying on audio replay are not silently dropped.
                node.status({ fill: "red", shape: "ring", text: "audio replay unsupported" });
                done("Playing recorded audio (audio_base64) is not supported over the WebSocket API. Provide text to speak instead.");
                return;
            }

            node.status({ fill: "blue", shape: "dot", text: `saying: "${textToSay.substring(0, 15)}..."` });

            const payload = {
                action: "tts",
                node_id: resolvedNodeId,
                text: textToSay,
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

                node.status({ fill: "green", shape: "dot", text: "finished" });
                send(msg);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "request failed" });
                done(`WebSocket request failed: ${err.message}`);
            }
        });
    }

    RED.nodes.registerType("gocalis-say", GocalisSayNode);
}
