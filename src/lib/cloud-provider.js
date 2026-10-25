import log from './log.js';
import throttle from 'lodash.throttle';

const anonymizeUsername = username => {
    if (/^player\d{2,7}$/i.test(username)) {
        return 'player';
    }
    return username;
};

// ICE servers — STUN lets peers discover their public IP.
// For users behind symmetric NAT we fall back to WebSocket automatically.
const ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
];

class CloudProvider {
    constructor (cloudHost, vm, username, projectId) {
        this.vm = vm;
        this.username = anonymizeUsername(username);
        this.projectId = projectId;
        this.cloudHost = cloudHost;

        this.connectionAttempts = 0;

        // Queued messages to send once WebSocket is open
        this.queuedData = [];

        // Map of peerId -> RTCPeerConnection
        this.peerConnections = {};

        // Map of peerId -> RTCDataChannel
        this.dataChannels = {};

        // Our own peer ID assigned by the server
        this.myPeerId = null;

        this.openConnection();

        // Throttle cloud variable sends to max 20/sec (50ms)
        this.sendCloudData = this._sendCloudData.bind(this);
    }

    // -------------------------------------------------------------------------
    // WebSocket (signaling + fallback)
    // -------------------------------------------------------------------------

    openConnection () {
        this.connectionAttempts += 1;

        try {
            if (!this.cloudHost ||
                (!this.cloudHost.includes('ws://') && !this.cloudHost.includes('wss://'))) {
                this.cloudHost = (location.protocol === 'http:' ? 'ws://' : 'wss://') + this.cloudHost;
            }
            this.connection = new WebSocket(this.cloudHost);
        } catch (e) {
            log.warn('Websocket support is not available in this browser', e);
            this.connection = null;
            return;
        }

        this.connection.onerror = this.onError.bind(this);
        this.connection.onmessage = this.onMessage.bind(this);
        this.connection.onopen = this.onOpen.bind(this);
        this.connection.onclose = this.onClose.bind(this);
    }

    onError (event) {
        log.error(`Websocket connection error: ${JSON.stringify(event)}`);
    }

    onOpen () {
        this.connectionAttempts = 1;
        this.writeToServer('handshake');
        log.info('Successfully connected to clouddata server.');

        this.queuedData.forEach(data => this.sendCloudData(data));
        this.queuedData = [];
    }

    onClose (e) {
        if (e && e.code === 4002) {
            log.info('Cloud username is invalid. Not reconnecting.');
            this.onInvalidUsername();
            return;
        }
        if (e && e.code === 4003) {
            log.info('The server is full. Not reconnecting.');
            return;
        }
        if (e && e.code === 4004) {
            log.info('Cloud variables are disabled for this project. Not reconnecting.');
            return;
        }
        log.info(`WebSocket connection closed with code ${e && e.code}`);
        const randomizedTimeout = this.randomizeDuration(this.exponentialTimeout());
        this.setTimeout(this.openConnection.bind(this), randomizedTimeout);
    }

    onMessage (event) {
        const messageString = event.data;

        // Handle newline-delimited cloud variable messages from server
        // (these arrive when a peer doesn't have a data channel open yet)
        messageString.split('\n').forEach(line => {
            if (!line) return;

            let data;
            try {
                data = JSON.parse(line);
            } catch (e) {
                return;
            }

            // --- SIGNALING MESSAGES ---
            if (data.action === 'assign_id') {
                this.myPeerId = data.peerId;
                log.info(`Assigned peer ID: ${this.myPeerId}`);
                return;
            }

            if (data.action === 'room_peers') {
                // We just joined — initiate offers to everyone already in the room
                data.peers.forEach(peerId => this.createOffer(peerId));
                return;
            }

            if (data.action === 'peer_joined') {
                // A new peer joined — they will send us an offer, just wait
                log.info(`Peer joined room: ${data.peerId}`);
                return;
            }

            if (data.action === 'peer_left') {
                this.closePeer(data.peerId);
                return;
            }

            if (data.action === 'offer') {
                this.handleOffer(data);
                return;
            }

            if (data.action === 'answer') {
                this.handleAnswer(data);
                return;
            }

            if (data.action === 'ice_candidate') {
                this.handleIceCandidate(data);
                return;
            }

            // --- CLOUD VARIABLE UPDATE (fallback path via WebSocket) ---
            if (data.method === 'set') {
                const parsed = this.parseMessage(data);
                this.vm.postIOData('cloud', parsed);
            }
        });
    }

    // -------------------------------------------------------------------------
    // WebRTC — peer connection management
    // -------------------------------------------------------------------------

    createPeerConnection (peerId) {
        const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
        this.peerConnections[peerId] = pc;

        pc.onicecandidate = (e) => {
            if (e.candidate) {
                this.sendSignal({
                    action: 'ice_candidate',
                    to: peerId,
                    candidate: e.candidate
                });
            }
        };

        pc.onconnectionstatechange = () => {
            log.info(`Peer ${peerId} connection state: ${pc.connectionState}`);
            if (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
                this.closePeer(peerId);
            }
        };

        return pc;
    }

    async createOffer (peerId) {
        const pc = this.createPeerConnection(peerId);

        // Unreliable + unordered = UDP-like. Stale variable updates
        // won't block fresh ones.
        const dc = pc.createDataChannel('cloud', {
            ordered: false,
            maxRetransmits: 0
        });
        this.setupDataChannel(peerId, dc);

        try {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            this.sendSignal({ action: 'offer', to: peerId, offer });
        } catch (e) {
            log.error(`Error creating offer for ${peerId}: ${e}`);
        }
    }

    async handleOffer (data) {
        const peerId = data.from;
        const pc = this.createPeerConnection(peerId);

        // The answering side receives the data channel
        pc.ondatachannel = (e) => {
            this.setupDataChannel(peerId, e.channel);
        };

        try {
            await pc.setRemoteDescription(new RTCSessionDescription(data.offer));
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            this.sendSignal({ action: 'answer', to: peerId, answer });
        } catch (e) {
            log.error(`Error handling offer from ${peerId}: ${e}`);
        }
    }

    async handleAnswer (data) {
        const pc = this.peerConnections[data.from];
        if (!pc) return;
        try {
            await pc.setRemoteDescription(new RTCSessionDescription(data.answer));
        } catch (e) {
            log.error(`Error handling answer from ${data.from}: ${e}`);
        }
    }

    async handleIceCandidate (data) {
        const pc = this.peerConnections[data.from];
        if (!pc) return;
        try {
            await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
        } catch (e) {
            log.error(`Error adding ICE candidate from ${data.from}: ${e}`);
        }
    }

    setupDataChannel (peerId, dc) {
        dc.onopen = () => {
            log.info(`Data channel open with peer ${peerId}`);
            this.dataChannels[peerId] = dc;
        };

        dc.onclose = () => {
            log.info(`Data channel closed with peer ${peerId}`);
            delete this.dataChannels[peerId];
        };

        dc.onerror = (e) => {
            log.error(`Data channel error with peer ${peerId}: ${e}`);
        };

        dc.onmessage = (e) => {
            try {
                const data = JSON.parse(e.data);
                if (data.method === 'set') {
                    const parsed = this.parseMessage(data);
                    this.vm.postIOData('cloud', parsed);
                }
            } catch (err) {
                log.error(`Error parsing data channel message: ${err}`);
            }
        };
    }

    closePeer (peerId) {
        if (this.dataChannels[peerId]) {
            this.dataChannels[peerId].close();
            delete this.dataChannels[peerId];
        }
        if (this.peerConnections[peerId]) {
            this.peerConnections[peerId].close();
            delete this.peerConnections[peerId];
        }
        log.info(`Closed peer connection: ${peerId}`);
    }

    // -------------------------------------------------------------------------
    // Sending — data channel first, WebSocket fallback
    // -------------------------------------------------------------------------

    sendSignal (msg) {
        if (this.connection && this.connection.readyState === WebSocket.OPEN) {
            this.connection.send(JSON.stringify(msg));
        }
    }

    _sendCloudData (data) {
        const openChannels = Object.values(this.dataChannels).filter(
            dc => dc.readyState === 'open'
        );

        if (openChannels.length > 0) {
            // Send directly to all peers via data channel (no server hop)
            openChannels.forEach(dc => {
                try {
                    dc.send(data.trim()); // data channels don't need the \n delimiter
                } catch (e) {
                    log.error(`Data channel send error: ${e}`);
                }
            });
        } else {
            // No data channels open yet — fall back to WebSocket relay
            if (this.connection && this.connection.readyState === WebSocket.OPEN) {
                this.connection.send(`${data}\n`);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Unchanged interface — VM calls these directly
    // -------------------------------------------------------------------------

    parseMessage (message) {
        const varData = {};
        switch (message.method) {
        case 'set': {
            varData.varUpdate = {
                name: message.name,
                value: message.value
            };
            break;
        }
        }
        return varData;
    }

    writeToServer (methodName, dataName, dataValue, dataNewName) {
        const msg = {};
        msg.method = methodName;
        msg.user = this.username;
        msg.project_id = this.projectId;

        if (dataName) msg.name = dataName;
        if (dataNewName) msg.new_name = dataNewName;
        if (typeof dataValue !== 'undefined' && dataValue !== null) msg.value = dataValue;

        const dataToWrite = JSON.stringify(msg);
        if (this.connection && this.connection.readyState === WebSocket.OPEN) {
            this.sendCloudData(dataToWrite);
        } else if (msg.method === 'create' || msg.method === 'delete' || msg.method === 'rename') {
            this.queuedData.push(dataToWrite);
        }
    }

    createVariable (name, value) {
        this.writeToServer('create', name, value);
    }

    updateVariable (name, value) {
        this.writeToServer('set', name, value);
    }

    renameVariable (oldName, newName) {
        this.writeToServer('rename', oldName, null, newName);
    }

    deleteVariable (name) {
        this.writeToServer('delete', name);
    }

    requestCloseConnection () {
        // Close all peer connections cleanly
        Object.keys(this.peerConnections).forEach(peerId => this.closePeer(peerId));

        if (this.connection &&
            this.connection.readyState !== WebSocket.CLOSING &&
            this.connection.readyState !== WebSocket.CLOSED) {
            log.info('Request close cloud connection without reconnecting');
            this.connection.onclose = () => {};
            this.connection.onerror = () => {};
            this.connection.close();
        }
        this.clear();
    }

    clear () {
        this.connection = null;
        this.vm = null;
        this.username = null;
        this.projectId = null;
        this.peerConnections = {};
        this.dataChannels = {};
        this.myPeerId = null;
        if (this._connectionTimeout) {
            clearTimeout(this._connectionTimeout);
            this._connectionTimeout = null;
        }
        this.connectionAttempts = 0;
    }

    exponentialTimeout () {
        return (Math.pow(2, Math.min(this.connectionAttempts, 5)) - 1) * 1000;
    }

    randomizeDuration (t) {
        return Math.random() * t;
    }

    setTimeout (fn, time) {
        log.info(`Reconnecting in ${(time / 1000).toFixed(1)}s, attempt ${this.connectionAttempts}`);
        this._connectionTimeout = window.setTimeout(fn, time);
    }

    onInvalidUsername () { /* no-op */ }
}

export default CloudProvider;
