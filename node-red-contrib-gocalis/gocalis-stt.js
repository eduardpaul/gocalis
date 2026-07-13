module.exports = function(RED) {
    function GocalisSttNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "default";
        this.nodeIdType = config.nodeIdType || "str";
        this.audioFile = config.audioFile;
        this.audioFileType = config.audioFileType || "str";
        this.requireSpeakerId = !!config.requireSpeakerId;
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

            let audioSource = null;
            if (node.audioFile && node.audioFile.trim() !== "") {
                try {
                    audioSource = RED.util.evaluateNodeProperty(node.audioFile, node.audioFileType, node, msg);
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "error evaluating property" });
                    done(`Failed to evaluate audio source property: ${err.message}`);
                    return;
                }
            } else {
                audioSource = msg.payload;
            }

            // The WS API's "asr" action transcribes a file already accessible to
            // the server (audio_file path); there is no upload channel. Buffers
            // therefore cannot be transcribed over WebSocket.
            if (Buffer.isBuffer(audioSource)) {
                node.status({ fill: "red", shape: "ring", text: "buffer unsupported" });
                done("Transcribing an audio Buffer is not supported over the WebSocket API. Provide a server-accessible file path string instead.");
                return;
            }

            if (typeof audioSource !== 'string' || audioSource.trim() === "") {
                node.status({ fill: "yellow", shape: "ring", text: "invalid input" });
                done("Input source must be a server-accessible audio file path string.");
                return;
            }
            const audioFile = audioSource;

            let requireSpeakerId = node.requireSpeakerId;
            if (msg.require_speaker_id !== undefined) {
                requireSpeakerId = !!msg.require_speaker_id;
            } else if (msg.requireSpeakerId !== undefined) {
                requireSpeakerId = !!msg.requireSpeakerId;
            } else if (msg.verify_speaker !== undefined) {
                requireSpeakerId = !!msg.verify_speaker;
            } else if (msg.verifySpeaker !== undefined) {
                requireSpeakerId = !!msg.verifySpeaker;
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
            }

            const priority = msg.priority !== undefined ? parseInt(msg.priority) : node.priority;

            node.status({ fill: "blue", shape: "dot", text: "transcribing..." });

            try {
                const asrResult = await node.server.request({
                    action: "asr",
                    node_id: resolvedNodeId,
                    audio_file: audioFile,
                    priority: priority
                }, {
                    expectEvents: ["asr_completed"],
                    nodeId: resolvedNodeId
                });

                if (asrResult.event === "error" || asrResult.status === "error") {
                    node.status({ fill: "red", shape: "ring", text: asrResult.message || "error" });
                    done(`Gocalis engine reported failure: ${asrResult.message || "error"}`);
                    return;
                }

                msg.transcription = asrResult.text;

                if (requireSpeakerId) {
                    node.status({ fill: "blue", shape: "dot", text: "identifying speaker..." });
                    const spkResult = await node.server.request({
                        action: "speaker_id",
                        node_id: resolvedNodeId,
                        audio_file: audioFile
                    }, {
                        expectEvents: ["speaker_id_completed"],
                        nodeId: resolvedNodeId
                    });

                    if (spkResult.event === "error" || spkResult.status === "error") {
                        node.status({ fill: "red", shape: "ring", text: spkResult.message || "error" });
                        done(`Speaker ID failed: ${spkResult.message || "error"}`);
                        return;
                    }
                    msg.speaker = spkResult.speaker;
                }

                msg.payload = {
                    status: "success",
                    transcription: msg.transcription,
                    speaker: msg.speaker,
                    node_id: resolvedNodeId
                };

                node.status({ fill: "green", shape: "dot", text: "done" });
                send(msg);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "request failed" });
                done(`WebSocket request failed: ${err.message}`);
            }
        });
    }

    RED.nodes.registerType("gocalis-stt", GocalisSttNode);
}
