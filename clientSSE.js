const EventSource = require('eventsource');
const pushFile = require('./dataLakeConn');
const axios = require('axios');
const mqtt = require('mqtt');
const WebSocket = require('ws');

// ==================== SSE Data Collector ====================
function getSSEData(url, channel, type) {
    console.log(`Starting SSE connection for ${channel}`);

    const eventSource = new EventSource(url);

    eventSource.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            console.log(`SSE Data - ${channel}:`, data);
            pushFile(JSON.stringify(data), channel, type);
        } catch (error) {
            console.error(`Error processing SSE data for ${channel}:`, error);
        }
    };

    eventSource.onerror = function(error) {
        console.error(`SSE Error for ${channel}:`, error);
    };

    eventSource.onopen = function() {
        console.log(`SSE connection opened for ${channel}`);
    };

    return eventSource;
}

// ==================== MQTT Data Collector ====================
let mqttClient; // Global variable to store client reference

function getMqttData(host, type) {
    console.log('Starting MQTT connection');

    const options = {
        host: host,
        port: 1883,
        username: 'DIuser',
        password: 'datoveInzinierstvo2025',
        clean: true,
        connectTimeout: 4000,
        reconnectPeriod: 1000
    };

    mqttClient = mqtt.connect('mqtt://91.99.234.80', options);

    mqttClient.on('connect', () => {
        console.log('Connected to MQTT broker');

        const topics = ['hbo/viewers', 'joj-cinema/viewers', 'film-plus/viewers'];
        topics.forEach(topic => {
            mqttClient.subscribe(topic, (err) => {
                if (err) {
                    console.error(`Subscribe error for ${topic}:`, err);
                } else {
                    console.log(`Subscribed to ${topic}`);
                }
            });
        });
    });

    mqttClient.on('message', (topic, message) => {
        try {
            const channel = topic.split('/')[0];
            const viewerCount = `{"viewers": ${parseInt(message.toString())}, "timestamp": "${new Date().toISOString()}"}`;
            console.log(`MQTT Data - ${channel}: ${viewerCount}`);
            pushFile(viewerCount, channel, type);
        } catch (error) {
            console.error(`Error processing MQTT message for ${topic}:`, error);
        }
    });

    mqttClient.on('error', (error) => {
        console.error('MQTT Error:', error);
    });

    mqttClient.on('close', () => {
        console.log('MQTT connection closed');
    });
}

// ==================== HTTP Stream Data Collector ====================
async function getStreamedData(url, channel, type) {
    console.log(`Starting HTTP stream for ${channel}`);

    try {
        const response = await axios.get(url, {
            responseType: 'stream'
        });

        response.data.on('data', (chunk) => {
            try {
                const data = JSON.parse(chunk);
                console.log(`Stream Data - ${channel}:`, data);
                pushFile(JSON.stringify(data), channel, type);
            } catch (error) {
                // Ignore parsing errors for partial chunks
            }
        });

        response.data.on('error', (error) => {
            console.error(`Stream error for ${channel}:`, error);
        });

        response.data.on('end', () => {
            console.log(`Stream ended for ${channel}`);
        });

    } catch (error) {
        console.error(`Error starting stream for ${channel}:`, error);
    }
}

// ==================== WebSocket Data Collector ====================
const wsConnections = new Map();
const wsChannels = ["hbo-2", "filmbox", "nova-cinema"];
const wsServerBase = "ws://37.9.171.199:4444/channel";

function connectToWebSocketChannel(channel) {
    const url = `${wsServerBase}/${channel}`;
    console.log(`Connecting WebSocket to ${channel}`);

    const ws = new WebSocket(url);

    ws.on('open', () => {
        console.log(`WebSocket connected to ${channel}`);
        wsConnections.set(channel, ws);
    });

    ws.on('message', (data) => {
        try {
            const jsonData = JSON.parse(data);
            console.log(`WebSocket Data - ${jsonData.channel}: ${jsonData.viewers}`);
            const formattedData = {
                channel: jsonData.channel,
                viewers: jsonData.viewers,
            };
            pushFile(JSON.stringify(formattedData), jsonData.channel, 'viewership');
        } catch (error) {
            console.error(`Error processing WebSocket data from ${channel}:`, error);
        }
    });

    ws.on('close', () => {
        console.log(`WebSocket connection closed for ${channel}`);
        wsConnections.delete(channel);

        // Reconnect after delay
        setTimeout(() => {
            connectToWebSocketChannel(channel);
        }, 5000);
    });

    ws.on('error', (error) => {
        console.error(`WebSocket error for ${channel}:`, error);
    });
}

function startWebSocketCollectors() {
    console.log('Starting all WebSocket connections...');
    wsChannels.forEach(channel => {
        connectToWebSocketChannel(channel);
    });
}

function stopWebSocketCollectors() {
    console.log('Stopping all WebSocket connections...');
    wsConnections.forEach((ws, channel) => {
        ws.close();
    });
    wsConnections.clear();
}

// ==================== Main Functions ====================
let eventSources = [];

function startAllCollectors() {
    console.log('🚀 Starting all data collectors...\n');

    // Start SSE collectors
    /*try {
        const sse1 = getSSEData('http://dgx.uvt.tuke.sk:5001/subscribe', 'discovery-channel-bbc-natgeo', 'viewership');
        eventSources.push(sse1);
    } catch (error) {
        console.error('Failed to start SSE collector:', error);
    }*/

    try {
        const sseDajto = getSSEData('http://3.79.27.222:3000/dajto/viewership', 'dajto', 'viewership');
        const ssePrima = getSSEData('http://3.79.27.222:3000/prima-sk/viewership', 'prima-sk', 'viewership');
        const sseKrimi = getSSEData('http://3.79.27.222:3000/markiza-krimi/viewership', 'markiza-krimi', 'viewership');

        eventSources.push(sseDajto, ssePrima, sseKrimi)
    } catch (error) {
        console.error('Failed to start SSE collector:', error)
    }

    // Start MQTT collector
    try {
        getMqttData('91.99.234.80', 'viewership');
    } catch (error) {
        console.error('Failed to start MQTT collector:', error);
    }

    // Start HTTP stream collector
    try {
        getStreamedData('https://merry-briefly-dinosaur.ngrok-free.app/hbo_3/viewers', 'hbo3', 'viewership');
        getStreamedData('https://merry-briefly-dinosaur.ngrok-free.app/cinemax/viewers', 'cinemax', 'viewership');
        getStreamedData('https://merry-briefly-dinosaur.ngrok-free.app/cinemax_2/viewers', 'cinemax2', 'viewership');
        getStreamedData('http://dgx.uvt.tuke.sk:5001/subscribe', 'discovery-bbc-natgeo', 'viewership');
    } catch (error) {
        console.error('Failed to start HTTP stream collector:', error);
    }

    // Start WebSocket collectors
    try {
        startWebSocketCollectors();
    } catch (error) {
        console.error('Failed to start WebSocket collectors:', error);
    }

    console.log('\n✅ All collectors initialized');
}

function stopAllCollectors() {
    console.log('\n🛑 Stopping all data collectors...');

    // Close EventSources
    eventSources.forEach(es => {
        if (es) es.close();
    });

    // Close MQTT client
    if (mqttClient) {
        mqttClient.end();
    }

    // Close WebSocket connections
    stopWebSocketCollectors();

    console.log('✅ All collectors stopped');
}

// ==================== Start Application ====================
// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\n🔄 Received SIGINT. Shutting down gracefully...');
    stopAllCollectors();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n\n🔄 Received SIGTERM. Shutting down gracefully...');
    stopAllCollectors();
    process.exit(0);
});

// Start all collectors
startAllCollectors();

console.log('📊 Data collection system is running...');
console.log('Press Ctrl+C to stop all collectors\n');

module.exports = {
    startAllCollectors,
    stopAllCollectors
};