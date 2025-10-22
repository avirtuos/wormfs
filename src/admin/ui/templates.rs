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
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        .metric-card {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: var(--shadow);
        }

        .metric-label {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }

        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--text);
        }

        .metric-unit {
            font-size: 1rem;
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
                    <div class="metric-label">Read Byte Rate (60s avg)</div>
                    <div class="metric-value">
                        <span x-text="formatByteRate(readByteRate.avg, readByteRate.peak)"></span>
                        <span class="metric-unit">Mbps</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Write Byte Rate (60s avg)</div>
                    <div class="metric-value">
                        <span x-text="formatByteRate(writeByteRate.avg, writeByteRate.peak)"></span>
                        <span class="metric-unit">Mbps</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Write Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filesystem.write_ops.total'])">
                        <span class="metric-unit">ops</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Read Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filesystem.read_ops.total'])">
                        <span class="metric-unit">ops</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">I/O Amplification Ratio</div>
                    <div class="metric-value" x-text="calculateAmplification()"></div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">RMW Operations</div>
                    <div class="metric-value" x-text="formatNumber(metrics['filestore.rmw_operations.total'])">
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
            <div class="placeholder">
                <p>⚙️ Configuration viewer coming soon</p>
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
                wsConnected: false,
                ws: null,
                components: {},  // Component-based metric discovery
                metadataMetrics: [],  // All metadata_store metrics
                filesystemMetrics: [],  // All filesystem metrics
                filestoreMetrics: [],  // All filestore metrics
                bufferedMemoryMetrics: [],  // All filesystem.buffered_memory metrics

                // Byte rate tracking
                readByteRate: { avg: 0, peak: 0 },
                writeByteRate: { avg: 0, peak: 0 },
                byteRateHistory: [],  // Array of {timestamp, readBytes, writeBytes}
                previousReadBytes: 0,
                previousWriteBytes: 0,
                rateUpdateInterval: null,

                init() {
                    this.connectWebSocket();
                    this.fetchMetrics();  // Initial fetch
                    this.fetchComponents();  // Fetch available components
                    this.startByteRateTracking();  // Start byte rate updates every second
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

                updateMetrics(newMetrics) {
                    // Extract values from metric objects
                    Object.keys(newMetrics).forEach(key => {
                        if (typeof newMetrics[key] === 'object' && newMetrics[key].value !== undefined) {
                            this.metrics[key] = newMetrics[key].value;
                        } else {
                            this.metrics[key] = newMetrics[key];
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

                // Smart formatting based on metric name
                formatMetricValue(metricName) {
                    const value = this.metrics[metricName];
                    if (!value) return '0';

                    if (metricName.endsWith('.bytes')) {
                        return this.formatBytes(value);
                    } else if (metricName.endsWith('.latency')) {
                        return this.formatLatency(value);
                    } else {
                        return this.formatNumber(value) + ' operations';
                    }
                },

                calculateAmplification() {
                    // Physical I/O: actual bytes read/written to storage
                    const physicalWriteBytes = this.metrics['filestore.stripe_write.bytes'] || 0;
                    const physicalReadBytes = this.metrics['filestore.stripe_read.bytes'] || 0;
                    const physicalBytes = physicalWriteBytes + physicalReadBytes;

                    // Logical I/O: bytes requested by user operations
                    const logicalWriteBytes = this.metrics['filesystem.write_ops.bytes'] || 0;
                    const logicalReadBytes = this.metrics['filesystem.read_ops.bytes'] || 0;
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
                    const currentReadBytes = this.metrics['filesystem.read_ops.bytes'] || 0;
                    const currentWriteBytes = this.metrics['filesystem.write_ops.bytes'] || 0;
                    const now = Date.now();

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

                    // Keep only last 60 seconds
                    const cutoffTime = now - (60 * 1000);
                    this.byteRateHistory = this.byteRateHistory.filter(
                        entry => entry.timestamp > cutoffTime
                    );

                    // Calculate avg and peak over last 60 seconds
                    if (this.byteRateHistory.length > 0) {
                        const readRates = this.byteRateHistory.map(e => e.readMbps);
                        const writeRates = this.byteRateHistory.map(e => e.writeMbps);

                        this.readByteRate = {
                            avg: readRates.reduce((a, b) => a + b, 0) / readRates.length,
                            peak: Math.max(...readRates)
                        };

                        this.writeByteRate = {
                            avg: writeRates.reduce((a, b) => a + b, 0) / writeRates.length,
                            peak: Math.max(...writeRates)
                        };
                    }

                    // Update previous values
                    this.previousReadBytes = currentReadBytes;
                    this.previousWriteBytes = currentWriteBytes;
                },

                formatByteRate(avg, peak) {
                    if (!avg && !peak) return '0 (Peak: 0)';
                    return `${avg.toFixed(2)} (Peak: ${peak.toFixed(2)})`;
                }
            };
        }
    </script>
</body>
</html>
"#;
