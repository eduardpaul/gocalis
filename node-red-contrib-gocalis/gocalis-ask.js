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

    function GocalisAskNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "default";
        this.nodeIdType = config.nodeIdType || "str";
        this.promptText = config.promptText;
        this.promptTextType = config.promptTextType || "str";
        this.bargeIn = config.bargeIn !== false;
        this.requireSpeakerId = !!config.requireSpeakerId;
        this.outputFormat = config.outputFormat || "both";
        this.vadTimeout = parseFloat(config.vadTimeout) || 10.0;
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

            let prompt = "";
            if (node.promptText && node.promptText.trim() !== "") {
                try {
                    const rawVal = RED.util.evaluateNodeProperty(node.promptText, node.promptTextType, node, msg);
                    if (typeof rawVal === 'string') {
                        prompt = interpolate(rawVal, msg);
                    } else if (rawVal !== undefined && rawVal !== null) {
                        prompt = typeof rawVal === 'object' ? JSON.stringify(rawVal) : String(rawVal);
                    }
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating property" });
                    done(`Failed to evaluate prompt property: ${err.message}`);
                    return;
                }
            } else {
                if (typeof msg.payload === 'string') {
                    prompt = msg.payload;
                } else {
                    prompt = "";
                }
            }

            if (typeof prompt !== 'string') {
                prompt = "";
            }

            const contextId = msg.context_id || `nr_ask_${Math.floor(Math.random() * 1000000)}`;

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

            node.status({ fill: "blue", shape: "dot", text: `asking: "${prompt.substring(0, 15)}..."` });

            let bargeIn = node.bargeIn;
            if (msg.barge_in !== undefined) {
                bargeIn = !!msg.barge_in;
            } else if (msg.bargeIn !== undefined) {
                bargeIn = !!msg.bargeIn;
            } else if (msg.interrupt !== undefined) {
                bargeIn = !!msg.interrupt;
            } else if (msg.interruption !== undefined) {
                bargeIn = !!msg.interruption;
            }

            let requireSpeakerId = node.requireSpeakerId;
            if (msg.require_speaker_id !== undefined) {
                requireSpeakerId = !!msg.require_speaker_id;
            } else if (msg.requireSpeakerId !== undefined) {
                requireSpeakerId = !!msg.requireSpeakerId;
            } else if (msg.verify_speaker !== undefined) {
                requireSpeakerId = !!msg.verify_speaker;
            } else if (msg.verifySpeaker !== undefined) {
                requireSpeakerId = !!msg.verifySpeaker;
            } else if (msg.speaker_id !== undefined) {
                requireSpeakerId = !!msg.speaker_id;
            } else if (msg.speakerId !== undefined) {
                requireSpeakerId = !!msg.speakerId;
            }

            let outputFormat = node.outputFormat;
            if (msg.output_format !== undefined) {
                outputFormat = msg.output_format;
            } else if (msg.outputFormat !== undefined) {
                outputFormat = msg.outputFormat;
            }

            let vadTimeout = node.vadTimeout;
            if (msg.vad_timeout_seconds !== undefined) {
                vadTimeout = parseFloat(msg.vad_timeout_seconds);
            } else if (msg.vadTimeout !== undefined) {
                vadTimeout = parseFloat(msg.vadTimeout);
            }

            const payload = {
                action: "ask",
                context_id: contextId,
                node_id: resolvedNodeId,
                text: prompt,
                barge_in: bargeIn,
                require_speaker_id: requireSpeakerId,
                output_format: outputFormat,
                vad_timeout_seconds: vadTimeout,
                priority: msg.priority !== undefined ? parseInt(msg.priority) : node.priority
            };

            try {
                const result = await node.server.request(payload, {
                    expectEvents: ["ask_completed"],
                    nodeId: resolvedNodeId,
                    timeoutMs: 300000
                });

                msg.status = result.status;
                msg.transcription = result.text;
                msg.speaker = result.speaker;
                msg.node_id = result.node_id || resolvedNodeId;
                msg.payload = result;

                if (result.event === "error") {
                    node.status({ fill: "red", shape: "ring", text: result.message || "error" });
                    done(`Gocalis engine reported failure: ${result.message || "error"}`);
                    return;
                }

                if (result.status === "success") {
                    node.status({ fill: "green", shape: "dot", text: `success: "${result.text || ''}"` });
                    send([msg, null]);
                    done();
                } else if (result.status === "silence_timeout") {
                    node.status({ fill: "yellow", shape: "ring", text: "silence timeout" });
                    send([null, msg]);
                    done();
                } else if (result.status === "verification_failed") {
                    node.status({ fill: "red", shape: "ring", text: "auth failed" });
                    send([null, msg]);
                    done();
                } else {
                    node.status({ fill: "red", shape: "ring", text: result.status });
                    done(`Gocalis engine reported failure: ${result.message || result.status}`);
                }
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "request failed" });
                done(`WebSocket request failed: ${err.message}`);
            }
        });
    }

    RED.nodes.registerType("gocalis-ask", GocalisAskNode);
}
