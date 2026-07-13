module.exports = function(RED) {
    function GocalisPlayNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "default";
        this.nodeIdType = config.nodeIdType || "str";
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

            // Resolve the recording to play. Prefer the top-level audio fields
            // (as produced by gocalis-ask / the wake-trigger AutoAsk output), and
            // fall back to a recording nested in msg.payload (the whole ask result
            // object). Must be a base64-encoded PCM16 WAV.
            let audioBase64 = msg.audio_wav_base64 || msg.audio_base64;
            if (!audioBase64 && msg.payload && typeof msg.payload === 'object') {
                audioBase64 = msg.payload.audio_wav_base64 || msg.payload.recording;
            }
            if (!audioBase64 && typeof msg.recording === 'string') {
                audioBase64 = msg.recording;
            }

            if (!audioBase64 || typeof audioBase64 !== 'string') {
                node.status({ fill: "yellow", shape: "ring", text: "no audio" });
                done("No recording to play. Provide msg.audio_wav_base64 (base64 PCM16 WAV).");
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

            node.status({ fill: "blue", shape: "dot", text: `playing on ${resolvedNodeId}` });

            const payload = {
                action: "play",
                node_id: resolvedNodeId,
                audio_wav_base64: audioBase64,
                priority: msg.priority !== undefined ? parseInt(msg.priority) : node.priority
            };

            try {
                const result = await node.server.request(payload, {
                    expectEvents: ["play_completed"],
                    nodeId: resolvedNodeId,
                    timeoutMs: 300000
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

    RED.nodes.registerType("gocalis-play", GocalisPlayNode);
}
