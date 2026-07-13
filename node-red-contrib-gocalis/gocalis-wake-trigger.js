module.exports = function(RED) {
    function GocalisWakeTriggerNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.nodeId = config.nodeId || "all";

        const node = this;

        if (node.server) {
            node.status({ fill: "blue", shape: "ring", text: "connecting..." });

            const eventHandler = (message) => {
                const msgNodeId = message.node_id || "default";
                if (node.nodeId !== "all" && node.nodeId !== msgNodeId) {
                    return;
                }

                // A single "wake" event is raised per trigger. Without AutoAsk it
                // carries only the device; with AutoAsk it additionally carries the
                // ASR result (transcription/speaker/status) and, optionally, a
                // base64 WAV recording. Port 1 always emits the wake/device info;
                // port 2 emits the captured command when AutoAsk data is present.
                if (message.event === "wake" || message.event === "wake_detected") {
                    const isAutoAsk = message.auto_ask === true;
                    node.status({ fill: "green", shape: "dot", text: `${msgNodeId}: wake heard` });

                    const wakeMsg = {
                        topic: "gocalis/wake",
                        node_id: msgNodeId,
                        payload: {
                            event: "wake_detected",
                            node_id: msgNodeId,
                            keyword: message.keyword,
                            auto_ask: isAutoAsk,
                            timestamp: message.timestamp
                        }
                    };

                    let commandMsg = null;
                    if (isAutoAsk) {
                        commandMsg = {
                            topic: "gocalis/command",
                            status: message.status,
                            transcription: message.text,
                            speaker: message.speaker,
                            node_id: msgNodeId,
                            payload: {
                                event: "command_received",
                                node_id: msgNodeId,
                                status: message.status,
                                transcription: message.text,
                                speaker: message.speaker,
                                recording: message.recording,
                                sample_rate: message.sample_rate,
                                timestamp: message.timestamp
                            }
                        };
                    }

                    node.send([wakeMsg, commandMsg]);

                    setTimeout(() => {
                        node.status({ fill: "green", shape: "ring", text: "standby" });
                    }, 3000);
                } else if (message.event === "state_change") {
                    const state = message.new_state;
                    const suffix = node.nodeId === "all" ? ` (${msgNodeId})` : "";
                    if (state === "IDLE") {
                        node.status({ fill: "green", shape: "ring", text: `standby${suffix}` });
                    } else if (state === "SPEAKING") {
                        node.status({ fill: "blue", shape: "dot", text: `speaking${suffix}` });
                    } else if (state === "LISTENING") {
                        node.status({ fill: "yellow", shape: "dot", text: `listening${suffix}` });
                    } else if (state === "PROCESSING") {
                        node.status({ fill: "purple", shape: "dot", text: `processing${suffix}` });
                    } else if (state === "CHALLENGING") {
                        node.status({ fill: "red", shape: "dot", text: `verifying...${suffix}` });
                    }
                }
            };

            node.server.on('gocalis-event', eventHandler);

            node.status({ fill: "green", shape: "ring", text: "standby" });

            node.on('close', function() {
                if (node.server) {
                    node.server.removeListener('gocalis-event', eventHandler);
                }
            });
        } else {
            node.status({ fill: "red", shape: "ring", text: "missing config" });
        }
    }

    RED.nodes.registerType("gocalis-wake-trigger", GocalisWakeTriggerNode);
}
