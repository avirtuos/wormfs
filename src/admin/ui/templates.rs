//! HTML templates for admin UI.
//!
//! Templates are embedded in the binary using include_str!.

/// Main admin UI HTML template.
pub const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WormFS Admin</title>
    <script src="https://unpkg.com/alpinejs@3.13.3/dist/cdn.min.js" defer></script>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --primary: #2563eb;
            --primary-dark: #1e40af;
            --bg: #f9fafb;
            --card-bg: #ffffff;
            --text: #111827;
            --text-secondary: #6b7280;
            --border: #e5e7eb;
            --success: #10b981;
            --warning: #f59e0b;
            --error: #ef4444;
            --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1);
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 1rem;
        }

        header {
            background: var(--card-bg);
            border-bottom: 1px solid var(--border);
            padding: 1rem 0;
            margin-bottom: 2rem;
        }

        .header-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        h1 {
            font-size: 1.5rem;
            font-weight: 700;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            background: var(--success);
            color: white;
            border-radius: 0.375rem;
            font-size: 0.875rem;
            font-weight: 500;
        }

        .tabs {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border);
        }

        .tab-button {
            padding: 0.75rem 1.5rem;
            background: none;
            border: none;
            border-bottom: 2px solid transparent;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 0.875rem;
            font-weight: 500;
            transition: all 0.2s;
        }

        .tab-button:hover {
            color: var(--text);
            border-bottom-color: var(--border);
        }

        .tab-button.active {
            color: var(--primary);
            border-bottom-color: var(--primary);
        }

        .tab-panel {
            display: none;
        }

        .tab-panel.active {
            display: block;
        }

        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .metric-card {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 0.5rem;
            padding: 1rem;
            box-shadow: var(--shadow);
        }

        .metric-label {
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }

        .metric-value {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--text);
        }

        .metric-unit {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-left: 0.25rem;
        }

        .metrics-section {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
        }

        .section-title {
            font-size: 1.125rem;
            font-weight: 600;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border);
        }

        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 0.75rem 0;
            border-bottom: 1px solid var(--border);
        }

        .metric-row:last-child {
            border-bottom: none;
        }

        .metric-name {
            color: var(--text);
            font-family: 'Courier New', monospace;
            font-size: 0.875rem;
        }

        .metric-val {
            font-weight: 600;
            color: var(--primary);
        }

        .connection-status {
            position: fixed;
            bottom: 1rem;
            right: 1rem;
            padding: 0.5rem 1rem;
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 0.375rem;
            box-shadow: var(--shadow);
            font-size: 0.875rem;
        }

        .status-dot {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 0.5rem;
        }

        .status-dot.connected {
            background: var(--success);
        }

        .status-dot.disconnected {
            background: var(--error);
        }

        .placeholder {
            text-align: center;
            padding: 3rem;
            color: var(--text-secondary);
        }

        .format-bytes {
            font-family: 'Courier New', monospace;
        }

        .warning-banner {
            background-color: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 0.5rem;
            padding: 1rem 1.5rem;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .warning-banner-icon {
            font-size: 1.5rem;
        }

        .warning-banner-content {
            flex: 1;
        }

        .warning-banner-title {
            font-weight: 600;
            color: #856404;
            margin-bottom: 0.25rem;
        }

        .warning-banner-message {
            color: #856404;
            font-size: 0.875rem;
        }

        .warning-banner-count {
            font-weight: 700;
            color: #d39e00;
        }

        .graph-container {
            background: var(--card-bg);
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: var(--shadow);
            margin-bottom: 2rem;
        }

        .graph-title {
            font-size: 1.125rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: var(--text);
        }

        /* Config Display Styles */
        .config-container {
            padding: 1.5rem;
        }

        .config-section {
            background: var(--card-bg);
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
        }

        .config-section-title {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: var(--text);
            border-bottom: 2px solid var(--border);
            padding-bottom: 0.5rem;
        }

        .config-grid {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }

        .config-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
            padding: 0.75rem;
            background: var(--bg);
            border-radius: 0.375rem;
            align-items: start;
        }

        .config-key {
            font-weight: 500;
            color: var(--text);
            position: relative;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .config-value {
            color: var(--text-secondary);
            font-family: 'Courier New', monospace;
            word-break: break-word;
        }

        .config-array-item {
            padding: 0.25rem 0;
            border-left: 2px solid var(--border);
            padding-left: 0.5rem;
            margin-bottom: 0.25rem;
        }

        /* Tooltip Styles */
        .tooltip-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 1rem;
            height: 1rem;
            font-size: 0.75rem;
            cursor: help;
            position: relative;
            opacity: 0.6;
            transition: opacity 0.2s;
        }

        .tooltip-icon:hover {
            opacity: 1;
        }

        .tooltip-content {
            position: absolute;
            left: 0;
            top: 100%;
            margin-top: 0.5rem;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 0.75rem 1rem;
            border-radius: 0.375rem;
            font-size: 0.875rem;
            font-weight: normal;
            line-height: 1.4;
            max-width: 20rem;
            z-index: 1000;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            white-space: normal;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        }

        .tooltip-content::before {
            content: '';
            position: absolute;
            bottom: 100%;
            left: 1rem;
            border: 0.375rem solid transparent;
            border-bottom-color: rgba(0, 0, 0, 0.9);
        }
    </style>
</head>
<body x-data="adminApp()">
    <header>
        <div class="container">
            <div class="header-content">
                <h1>🗂️ WormFS Admin Console</h1>
                <div class="status-badge">
                    <span>●</span>
                    <span>System Online</span>
                </div>
            </div>
        </div>
    </header>

    <div class="container">
        <div class="tabs">
            <button class="tab-button" :class="{ 'active': activeTab === 'monitoring' }" @click="activeTab = 'monitoring'">
                📊 Monitoring
            </button>
            <button class="tab-button" :class="{ 'active': activeTab === 'config' }" @click="activeTab = 'config'">
                ⚙️ Configuration
            </button>
            <button class="tab-button" :class="{ 'active': activeTab === 'health' }" @click="activeTab = 'health'">
                ❤️ Health
            </button>
            <button class="tab-button" :class="{ 'active': activeTab === 'logs' }" @click="activeTab = 'logs'">
                📝 Logs
            </button>
            <button class="tab-button" :class="{ 'active': activeTab === 'network' }" @click="activeTab = 'network'">
                🌐 Network
            </button>
        </div>

        <!-- Monitoring Tab -->
        <div class="tab-panel" :class="{ 'active': activeTab === 'monitoring' }">
            <!-- Dropped Metrics Warning -->
            <div class="warning-banner" x-show="droppedMetrics > 0" x-cloak>
                <div class="warning-banner-icon">⚠️</div>
                <div class="warning-banner-content">
                    <div class="warning-banner-title">Metrics Dropped</div>
                    <div class="warning-banner-message">
                        <span class="warning-banner-count" x-text="formatNumber(droppedMetrics)"></span>
                        metrics have been dropped due to channel overflow.
                        Consider increasing <code>channel_buffer_size</code> in the metrics configuration.
                    </div>
                </div>
            </div>

            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Total Write Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filesystem.write_ops.total']?.value || 0)">
                        <span class="metric-unit">ops</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Read Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filesystem.read_ops.total']?.value || 0)">
                        <span class="metric-unit">ops</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">I/O Amplification Ratio</div>
                    <div class="metric-value" x-text="calculateAmplification()"></div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">RMW Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filestore.rmw_operations.total']?.value || 0)">
                        <span class="metric-unit">ops</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Metrics Dropped</div>
                    <div class="metric-value" x-text="formatNumber(droppedMetrics)">
                        <span class="metric-unit">total</span>
                    </div>
                </div>
            </div>

            <!-- Throughput Graph -->
            <div class="graph-container">
                <h3 class="graph-title">I/O Throughput (Last 5 Minutes)</h3>
                <div id="throughputGraph" style="width: 100%; height: 400px;"></div>
            </div>

            <div class="metrics-section">
                <h2 class="section-title">Filesystem Operations</h2>

                <!-- All metrics dynamically discovered from API -->
                <template x-for="metric in filesystemMetrics" :key="metric">
                    <div class="metric-row" x-show="metrics[metric] !== undefined && metrics[metric] !== 0">
                        <span class="metric-name" x-text="metric"></span>
                        <span class="metric-val"
                              :style="(metric.includes('error') || metric.includes('failed')) ? 'color: #ef4444;' : ''"
                              x-text="formatMetricValue(metric)"></span>
                    </div>
                </template>

                <!-- Fallback message if no metrics discovered -->
                <div x-show="filesystemMetrics.length === 0" class="metric-row">
                    <span class="metric-name" style="color: var(--text-secondary);">No metrics available</span>
                    <span class="metric-val">—</span>
                </div>
            </div>

            <div class="metrics-section">
                <h2 class="section-title">FileStore Operations</h2>

                <!-- All metrics dynamically discovered from API -->
                <template x-for="metric in filestoreMetrics" :key="metric">
                    <div class="metric-row" x-show="metrics[metric] !== undefined && metrics[metric] !== 0">
                        <span class="metric-name" x-text="metric"></span>
                        <span class="metric-val"
                              :style="(metric.includes('error') || metric.includes('failed')) ? 'color: #ef4444;' : ''"
                              x-text="formatMetricValue(metric)"></span>
                    </div>
                </template>

                <!-- Fallback message if no metrics discovered -->
                <div x-show="filestoreMetrics.length === 0" class="metric-row">
                    <span class="metric-name" style="color: var(--text-secondary);">No metrics available</span>
                    <span class="metric-val">—</span>
                </div>
            </div>

            <div class="metrics-section">
                <h2 class="section-title">BufferedFileHandle Memory Usage</h2>

                <!-- All metrics dynamically discovered from API -->
                <template x-for="metric in bufferedMemoryMetrics" :key="metric">
                    <div class="metric-row" x-show="metrics[metric] !== undefined && metrics[metric] !== 0">
                        <span class="metric-name" x-text="metric"></span>
                        <span class="metric-val"
                              :style="(metric.includes('error') || metric.includes('failed')) ? 'color: #ef4444;' : ''"
                              x-text="formatMetricValue(metric)"></span>
                    </div>
                </template>

                <!-- Fallback message if no metrics discovered -->
                <div x-show="bufferedMemoryMetrics.length === 0" class="metric-row">
                    <span class="metric-name" style="color: var(--text-secondary);">No metrics available</span>
                    <span class="metric-val">—</span>
                </div>
            </div>

            <div class="metrics-section">
                <h2 class="section-title">MetadataStore Operations</h2>

                <!-- All metrics dynamically discovered from API -->
                <template x-for="metric in metadataMetrics" :key="metric">
                    <div class="metric-row" x-show="metrics[metric] !== undefined && metrics[metric] !== 0">
                        <span class="metric-name" x-text="metric"></span>
                        <span class="metric-val"
                              :style="(metric.includes('error') || metric.includes('corrupt')) ? 'color: #ef4444;' : ''"
                              x-text="formatMetricValue(metric)"></span>
                    </div>
                </template>

                <!-- Fallback message if no metrics discovered -->
                <div x-show="metadataMetrics.length === 0" class="metric-row">
                    <span class="metric-name" style="color: var(--text-secondary);">No metrics available</span>
                    <span class="metric-val">—</span>
                </div>
            </div>
        </div>

        <!-- Configuration Tab -->
        <div class="tab-panel" :class="{ 'active': activeTab === 'config' }">
            <div class="config-container">
                <template x-for="(component, key) in config" :key="key">
                    <div class="config-section">
                        <h3 class="config-section-title" x-text="key.charAt(0).toUpperCase() + key.slice(1)"></h3>
                        <div class="config-grid">
                            <template x-for="(value, configKey) in component.values" :key="configKey">
                                <div class="config-row">
                                    <div class="config-key">
                                        <span x-text="configKey"></span>
                                        <span class="tooltip-icon" @mouseenter="$el.nextElementSibling.style.display='block'" @mouseleave="$el.nextElementSibling.style.display='none'">❓</span>
                                        <div class="tooltip-content" style="display: none;" x-text="component.descriptions[configKey]"></div>
                                    </div>
                                    <div class="config-value">
                                        <template x-if="Array.isArray(value)">
                                            <div>
                                                <template x-for="(item, idx) in value" :key="idx">
                                                    <div x-text="item" class="config-array-item"></div>
                                                </template>
                                            </div>
                                        </template>
                                        <template x-if="!Array.isArray(value)">
                                            <span x-text="typeof value === 'boolean' ? (value ? 'true' : 'false') : value"></span>
                                        </template>
                                    </div>
                                </div>
                            </template>
                        </div>
                    </div>
                </template>
            </div>
        </div>

        <!-- Health Tab -->
        <div class="tab-panel" :class="{ 'active': activeTab === 'health' }">
            <div class="placeholder">
                <p>❤️ Health monitoring coming soon</p>
            </div>
        </div>

        <!-- Logs Tab -->
        <div class="tab-panel" :class="{ 'active': activeTab === 'logs' }">
            <div class="placeholder">
                <p>📝 Log viewer coming soon</p>
            </div>
        </div>

        <!-- Network Tab -->
        <div class="tab-panel" :class="{ 'active': activeTab === 'network' }">
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Node ID</div>
                    <div class="metric-value" style="font-size: 1rem;" x-text="networkStatus?.local_node?.node_id || 'Unknown'"></div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Peers</div>
                    <div class="metric-value" x-text="networkStatus?.statistics?.total_peers || 0"></div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Connected Peers</div>
                    <div class="metric-value" x-text="networkStatus?.statistics?.connected_peers || 0"></div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Messages Sent</div>
                    <div class="metric-value" x-text="formatNumber(networkStatus?.statistics?.messages_sent || 0)"></div>
                </div>
            </div>

            <div class="metrics-section">
                <h2 class="section-title">Connected Peers</h2>
                <div class="table-container">
                    <template x-if="networkStatus?.peers && networkStatus.peers.length > 0">
                        <table class="metrics-table">
                            <thead>
                                <tr>
                                    <th>Node ID</th>
                                    <th>Peer ID</th>
                                    <th>Address</th>
                                    <th>State</th>
                                    <th>Last Heartbeat</th>
                                    <th>Sequence</th>
                                    <th>RTT</th>
                                </tr>
                            </thead>
                            <tbody>
                                <template x-for="peer in networkStatus.peers" :key="peer.peer_id">
                                    <tr>
                                        <td x-text="peer.node_id"></td>
                                        <td><code style="font-size: 0.75rem;" x-text="peer.peer_id"></code></td>
                                        <td x-text="peer.addresses?.[0] || 'N/A'"></td>
                                        <td>
                                            <span class="status-badge" :style="'background: ' + (peer.connection_state === 'Connected' ? 'var(--success)' : 'var(--error)')" x-text="peer.connection_state"></span>
                                        </td>
                                        <td x-text="formatTimestamp(peer.last_heartbeat)"></td>
                                        <td x-text="peer.heartbeat_sequence || 'N/A'"></td>
                                        <td x-text="peer.rtt_ms ? peer.rtt_ms + ' ms' : 'N/A'"></td>
                                    </tr>
                                </template>
                            </tbody>
                        </table>
                    </template>
                    <template x-if="!networkStatus?.peers || networkStatus.peers.length === 0">
                        <div class="placeholder">
                            <p>🌐 No peers connected</p>
                        </div>
                    </template>
                </div>
            </div>
        </div>
    </div>

    <div class="connection-status">
        <span class="status-dot" :class="wsConnected ? 'connected' : 'disconnected'"></span>
        <span x-text="wsConnected ? 'Live Updates' : 'Disconnected'"></span>
    </div>

    <script>
        function adminApp() {
            return {
                activeTab: 'monitoring',
                metrics: {},
                droppedMetrics: 0,
                config: {},  // System configuration
                networkStatus: null,  // Network status
                wsConnected: false,
                ws: null,
                components: {},  // Component-based metric discovery
                metadataMetrics: [],  // All metadata_store metrics
                filesystemMetrics: [],  // All filesystem metrics
                filestoreMetrics: [],  // All filestore metrics
                bufferedMemoryMetrics: [],  // All filesystem.buffered_memory metrics

                // Byte rate tracking and graphing
                byteRateHistory: [],  // Array of {timestamp, readMbps, writeMbps}
                previousReadBytes: 0,
                previousWriteBytes: 0,
                rateUpdateInterval: null,
                graphInitialized: false,
                componentRefreshInterval: null,

                init() {
                    this.connectWebSocket();
                    this.fetchMetrics();  // Initial fetch
                    this.fetchComponents();  // Fetch available components
                    this.fetchConfig();  // Fetch configuration
                    this.fetchNetworkStatus();  // Fetch network status
                    this.initGraph();  // Initialize Plotly graph
                    this.startByteRateTracking();  // Start byte rate updates every second
                    this.startComponentRefresh();  // Discover new metrics every 30 seconds
                    this.startNetworkStatusRefresh();  // Refresh network status every 5 seconds
                },

                connectWebSocket() {
                    const wsUrl = `ws://${window.location.host}/ws/metrics`;
                    this.ws = new WebSocket(wsUrl);

                    this.ws.onopen = () => {
                        console.log('WebSocket connected');
                        this.wsConnected = true;
                    };

                    this.ws.onmessage = (event) => {
                        try {
                            const data = JSON.parse(event.data);
                            if (data.metrics) {
                                this.updateMetrics(data.metrics);
                            }
                        } catch (e) {
                            console.error('Failed to parse WebSocket message:', e);
                        }
                    };

                    this.ws.onerror = (error) => {
                        console.error('WebSocket error:', error);
                        this.wsConnected = false;
                    };

                    this.ws.onclose = () => {
                        console.log('WebSocket closed, reconnecting in 3s...');
                        this.wsConnected = false;
                        setTimeout(() => this.connectWebSocket(), 3000);
                    };
                },

                async fetchMetrics() {
                    try {
                        const response = await fetch('/api/metrics');
                        const data = await response.json();
                        if (data.metrics) {
                            this.updateMetrics(data.metrics);
                        }
                        if (data.dropped_metrics !== undefined) {
                            this.droppedMetrics = data.dropped_metrics;
                        }
                    } catch (e) {
                        console.error('Failed to fetch metrics:', e);
                    }
                },

                async fetchComponents() {
                    try {
                        const response = await fetch('/api/metrics/components');
                        const data = await response.json();
                        if (data.components) {
                            this.components = data.components;

                            // Extract metadata_store metrics
                            this.metadataMetrics = (data.components['metadata_store'] || [])
                                .map(suffix => 'metadata_store.' + suffix)
                                .sort();

                            // Extract filesystem metrics (excluding buffered_memory which we'll handle separately)
                            this.filesystemMetrics = (data.components['filesystem'] || [])
                                .filter(suffix => !suffix.startsWith('buffered_memory.'))
                                .map(suffix => 'filesystem.' + suffix)
                                .sort();

                            // Extract filestore metrics
                            this.filestoreMetrics = (data.components['filestore'] || [])
                                .map(suffix => 'filestore.' + suffix)
                                .sort();

                            // Extract buffered_memory metrics (subcomponent of filesystem)
                            this.bufferedMemoryMetrics = (data.components['filesystem'] || [])
                                .filter(suffix => suffix.startsWith('buffered_memory.'))
                                .map(suffix => 'filesystem.' + suffix)
                                .sort();
                        }
                    } catch (e) {
                        console.error('Failed to fetch components:', e);
                    }
                },

                async fetchConfig() {
                    try {
                        const response = await fetch('/api/config');
                        const data = await response.json();
                        if (data) {
                            this.config = data;
                        }
                    } catch (e) {
                        console.error('Failed to fetch config:', e);
                    }
                },

                async fetchNetworkStatus() {
                    try {
                        const response = await fetch('/api/network/status');
                        const data = await response.json();
                        if (data) {
                            this.networkStatus = data;
                        }
                    } catch (e) {
                        console.error('Failed to fetch network status:', e);
                    }
                },

                startNetworkStatusRefresh() {
                    // Refresh network status every 5 seconds
                    setInterval(() => {
                        this.fetchNetworkStatus();
                    }, 5000);  // 5 seconds
                },

                startComponentRefresh() {
                    // Refresh component list every 30 seconds to discover new metrics
                    this.componentRefreshInterval = setInterval(() => {
                        this.fetchComponents();
                    }, 30000);  // 30 seconds
                },

                initGraph() {
                    const layout = {
                        autosize: true,
                        margin: { l: 70, r: 30, t: 30, b: 50 },
                        xaxis: {
                            title: 'Time',
                            type: 'date',
                            showgrid: true,
                            gridcolor: '#e5e7eb'
                        },
                        yaxis: {
                            title: 'Throughput (bytes/sec)',
                            showgrid: true,
                            gridcolor: '#e5e7eb',
                            rangemode: 'tozero'
                        },
                        showlegend: true,
                        legend: { x: 0.01, y: 0.99 },
                        plot_bgcolor: '#ffffff',
                        paper_bgcolor: '#ffffff'
                    };

                    const readTrace = {
                        x: [],
                        y: [],
                        mode: 'lines',
                        name: 'Read',
                        line: { color: '#3b82f6', width: 2 }
                    };

                    const writeTrace = {
                        x: [],
                        y: [],
                        mode: 'lines',
                        name: 'Write',
                        line: { color: '#10b981', width: 2 }
                    };

                    Plotly.newPlot('throughputGraph', [readTrace, writeTrace], layout, {
                        responsive: true,
                        displayModeBar: false
                    });

                    this.graphInitialized = true;
                },

                updateMetrics(newMetrics) {
                    // Store full metric objects (including value, unit, type)
                    Object.keys(newMetrics).forEach(key => {
                        if (typeof newMetrics[key] === 'object' && newMetrics[key].value !== undefined) {
                            // Store the full metric object with metadata
                            this.metrics[key] = newMetrics[key];
                        } else {
                            // Legacy/simple numeric value (WebSocket might send just numbers)
                            this.metrics[key] = { value: newMetrics[key], unit: 'Count', type: 'Counter' };
                        }
                    });
                },

                formatNumber(value) {
                    if (!value) return '0';
                    return Math.floor(value).toLocaleString();
                },

                formatBytes(bytes) {
                    if (!bytes) return '0 B';
                    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
                    if (bytes === 0) return '0 B';
                    const i = Math.floor(Math.log(bytes) / Math.log(1024));
                    return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + sizes[i];
                },

                formatLatency(seconds) {
                    if (!seconds) return '0ms';
                    if (seconds < 0.001) {
                        return (seconds * 1000000).toFixed(2) + 'μs';
                    } else if (seconds < 1) {
                        return (seconds * 1000).toFixed(2) + 'ms';
                    } else {
                        return seconds.toFixed(2) + 's';
                    }
                },

                formatTimestamp(timestamp) {
                    if (!timestamp) return 'N/A';
                    try {
                        const date = new Date(timestamp);
                        const now = new Date();
                        const diffSeconds = Math.floor((now - date) / 1000);

                        if (diffSeconds < 60) {
                            return `${diffSeconds}s ago`;
                        } else if (diffSeconds < 3600) {
                            return `${Math.floor(diffSeconds / 60)}m ago`;
                        } else {
                            return date.toLocaleTimeString();
                        }
                    } catch (e) {
                        return 'Invalid';
                    }
                },

                // Smart formatting based on metric unit metadata
                formatMetricValue(metricName) {
                    const metric = this.metrics[metricName];
                    if (!metric || metric.value === undefined) return '0';

                    const value = metric.value;
                    const unit = metric.unit || 'Count';

                    // Format based on unit type from backend
                    switch (unit) {
                        case 'Bytes':
                            return this.formatBytes(value);
                        case 'Kilobytes':
                            return this.formatBytes(value * 1024);
                        case 'Megabytes':
                            return this.formatBytes(value * 1024 * 1024);
                        case 'Gigabytes':
                            return this.formatBytes(value * 1024 * 1024 * 1024);
                        case 'Seconds':
                            return this.formatLatency(value);
                        case 'Milliseconds':
                            return this.formatLatency(value / 1000);
                        case 'Count':
                            return this.formatNumber(value);
                        case 'Operations':
                        case 'Requests':
                        case 'Events':
                            return this.formatNumber(value) + ' ' + unit.toLowerCase();
                        default:
                            return this.formatNumber(value) + ' ' + unit.toLowerCase();
                    }
                },

                calculateAmplification() {
                    // Physical I/O: actual bytes read/written to storage (including erasure coding overhead)
                    const physicalWriteBytes = this.metrics['filestore.stripe_write.bytes_encoded']?.value || 0;
                    const physicalReadBytes = this.metrics['filestore.stripe_read.bytes_encoded']?.value || 0;
                    const physicalBytes = physicalWriteBytes + physicalReadBytes;

                    // Logical I/O: bytes requested by user operations
                    const logicalWriteBytes = this.metrics['filesystem.write_ops.bytes']?.value || 0;
                    const logicalReadBytes = this.metrics['filesystem.read_ops.bytes']?.value || 0;
                    const logicalBytes = logicalWriteBytes + logicalReadBytes;

                    if (logicalBytes === 0) {
                        return '0.00x';
                    }

                    const ratio = physicalBytes / logicalBytes;
                    return ratio.toFixed(2) + 'x';
                },

                startByteRateTracking() {
                    // Update byte rates every second
                    this.rateUpdateInterval = setInterval(() => {
                        this.updateByteRates();
                    }, 1000);
                },

                updateByteRates() {
                    const currentReadBytes = this.metrics['filesystem.read_ops.bytes']?.value || 0;
                    const currentWriteBytes = this.metrics['filesystem.write_ops.bytes']?.value || 0;
                    const now = Date.now();

                    // Skip first sample - only establish baseline
                    // This prevents artificially large "peak" values from UI starting after system
                    if (this.previousReadBytes === 0 && this.previousWriteBytes === 0) {
                        this.previousReadBytes = currentReadBytes;
                        this.previousWriteBytes = currentWriteBytes;
                        return;
                    }

                    // Calculate rates in bits per second
                    const readBytesPerSecond = currentReadBytes - this.previousReadBytes;
                    const writeBytesPerSecond = currentWriteBytes - this.previousWriteBytes;

                    // Convert to Mbps (bits per second / 1,000,000)
                    const readMbps = (readBytesPerSecond * 8) / 1_000_000;
                    const writeMbps = (writeBytesPerSecond * 8) / 1_000_000;

                    // Add to history
                    this.byteRateHistory.push({
                        timestamp: now,
                        readMbps: readMbps,
                        writeMbps: writeMbps
                    });

                    // Keep only last 300 seconds (5 minutes)
                    const cutoffTime = now - (300 * 1000);
                    this.byteRateHistory = this.byteRateHistory.filter(
                        entry => entry.timestamp > cutoffTime
                    );

                    // Update previous values
                    this.previousReadBytes = currentReadBytes;
                    this.previousWriteBytes = currentWriteBytes;

                    // Update the graph with new data
                    this.updateGraph();
                },

                updateGraph() {
                    if (!this.graphInitialized || this.byteRateHistory.length === 0) {
                        return;
                    }

                    // Extract timestamps and rates
                    const timestamps = this.byteRateHistory.map(entry => new Date(entry.timestamp));
                    const readRates = this.byteRateHistory.map(entry => entry.readMbps * 125000); // Convert Mbps to bytes/sec
                    const writeRates = this.byteRateHistory.map(entry => entry.writeMbps * 125000);

                    // Update Plotly graph
                    Plotly.update('throughputGraph', {
                        x: [timestamps, timestamps],
                        y: [readRates, writeRates]
                    }, {}, [0, 1]);
                }
            };
        }
    </script>
</body>
</html>
"#;
