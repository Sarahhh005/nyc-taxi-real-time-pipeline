import React from 'react';
import { Activity, Server } from 'lucide-react';

export default function Header({ backendStatus }) {
  const isOnline = backendStatus?.status === 'online';

  return (
    <header className="app-header">
      <div className="brand-section">
        <div className="brand-icon">🚖</div>
        <div>
          <h1 className="brand-title">NYC Taxi Real-Time AI Agent</h1>
          <p className="brand-subtitle">Natural Language Analytics & Live ClickHouse Pipeline</p>
        </div>
      </div>

      <div className="status-badge" title={isOnline ? 'Agent API is online' : 'Backend is unreachable'}>
        <span className={`status-dot ${isOnline ? 'online' : 'offline'}`} />
        <span style={{ color: isOnline ? '#10b981' : '#ef4444' }}>
          {isOnline ? 'Agent API Active' : 'Backend Offline'}
        </span>
      </div>
    </header>
  );
}
