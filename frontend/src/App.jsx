import React, { useState, useEffect } from 'react';
import Header from './components/Header';
import ChatConsole from './components/ChatConsole';
import SystemServices from './components/SystemServices';
import { checkHealth } from './services/api';

export default function App() {
  const [backendStatus, setBackendStatus] = useState({ status: 'checking' });

  useEffect(() => {
    const runHealthCheck = async () => {
      const res = await checkHealth();
      setBackendStatus(res);
    };

    runHealthCheck();
    // Poll health status every 15 seconds
    const interval = setInterval(runHealthCheck, 15000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app-container">
      <Header backendStatus={backendStatus} />

      <main className="main-grid">
        <ChatConsole backendStatus={backendStatus} />
        <aside>
          <SystemServices />
        </aside>
      </main>

      <footer className="app-footer">
        NYC Taxi Real-Time Analytics & AI Pipeline — Developed with FastAPI, LangChain, ClickHouse, Apache Kafka & React
      </footer>
    </div>
  );
}
