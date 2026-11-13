module.exports = function(RED) {
    // Sends commands to one Gocalis plugin and passes on the events that
    // plugin reports. What the commands, their args and the events are is up
    // to the plugin type; this node only carries them.
    function GocalisPluginNode(config) {
        RED.nodes.createNode(this, config);
        this.server = RED.nodes.getNode(config.server);
        this.plugin = (config.plugin || "").trim();
        this.command = (config.command || "").trim();
        this.nodeId = (config.nodeId || "").trim();
        this.args = config.args || "";

        const node = this;

        if (!node.server) {
            node.status({ fill: "red", shape: "ring", text: "missing config" });
        } else {
            node.status({ fill: "grey", shape: "ring", text: "idle" });
        }

        // Everything the plugin reports comes out of output 2, whoever sent
        // the command that caused it (this node, another one, the dashboard).
        const eventHandler = (message) => {
            if (!node.plugin || message.plugin !== node.plugin) {
                return;
            }
            const failed = message.status === "error";
            const text = message.event === "error"
                ? (message.message || "error")
                : message.event + (message.reason ? ` (${message.reason})` : "");
            node.status({ fill: failed ? "red" : "green", shape: failed ? "ring" : "dot", text: text.substring(0, 60) });
            node.send([null, {
                topic: `gocalis/plugin/${node.plugin}`,
                payload: message,
                event: message.event,
                node_id: message.node_id
            }]);
        };

        if (node.server) {
            node.server.on('gocalis-event', eventHandler);
        }

        node.on('input', function(msg, send, done) {
            send = send || function() { node.send.apply(node, arguments); };
            done = done || function(err) { if (err) { node.error(err, msg); } };

            if (!node.server) {
                node.status({ fill: "red", shape: "ring", text: "missing config" });
                done("Missing Gocalis configuration");
                return;
            }
            if (!node.plugin) {
                node.status({ fill: "red", shape: "ring", text: "no plugin" });
                done("No plugin id configured");
                return;
            }

            // The command: msg.command, or msg.payload when it is a plain
            // word ("hangup"), or the configured one.
            let command = node.command;
            if (typeof msg.command === "string" && msg.command.trim() !== "") {
                command = msg.command.trim();
            } else if (typeof msg.payload === "string" && /^[a-z][a-z0-9_]*$/i.test(msg.payload.trim())) {
                command = msg.payload.trim();
            }
            if (!command) {
                node.status({ fill: "red", shape: "ring", text: "no command" });
                done("No command: configure one, or send it in msg.command");
                return;
            }

            // The args: the configured JSON object, with msg.args on top.
            let args = {};
            if (String(node.args).trim() !== "") {
                try {
                    args = JSON.parse(node.args);
                } catch (err) {
                    node.status({ fill: "red", shape: "ring", text: "bad args" });
                    done(`The configured args are not valid JSON: ${err.message}`);
                    return;
                }
            }
            if (msg.args !== undefined) {
                if (msg.args === null || typeof msg.args !== "object" || Array.isArray(msg.args)) {
                    done("msg.args must be an object");
                    return;
                }
                args = Object.assign({}, args, msg.args);
            }
            if (args === null || typeof args !== "object" || Array.isArray(args)) {
                node.status({ fill: "red", shape: "ring", text: "bad args" });
                done("The args must be a JSON object");
                return;
            }

            const payload = { action: "plugin", plugin: node.plugin, command: command };
            const target = msg.node_id !== undefined ? String(msg.node_id) : node.nodeId;
            if (target) {
                payload.node_id = target;
            }
            if (Object.keys(args).length > 0) {
                payload.args = args;
            }

            if (!node.server.sendWS(payload)) {
                node.status({ fill: "red", shape: "ring", text: "not connected" });
                done("WebSocket not connected");
                return;
            }
            node.status({ fill: "blue", shape: "dot", text: `${command} sent` });
            msg.payload = payload;
            send([msg, null]);
            done();
        });

        node.on('close', function() {
            if (node.server) {
                node.server.removeListener('gocalis-event', eventHandler);
            }
        });
    }

    RED.nodes.registerType("gocalis-plugin", GocalisPluginNode);
}
