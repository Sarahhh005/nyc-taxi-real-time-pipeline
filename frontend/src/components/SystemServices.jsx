import React from 'react';
import { ExternalLink, Database, Cpu, LayoutDashboard, Workflow } from 'lucide-react';

export default function SystemServices() {
  const services = [
    {
      name: 'Airflow Orchestrator',
      desc: 'Pipeline healthchecks & DAG workflows',
      port: ':8081',
      url: 'http://localhost:8081',
      icon: Workflow,
      color: '#38bdf8',
    },
    {
      name: 'ClickHouse Analytics DB',
      desc: 'High-performance real-time database',
      port: ':8123',
      url: 'http://localhost:8123/ping',
      icon: Database,
      color: '#f59e0b',
    },
    {
      name: 'Apache Superset',
      desc: 'Business intelligence dashboards',
      port: ':8088',
      url: 'http://localhost:8088',
      icon: LayoutDashboard,
      color: '#10b981',
    },
    {
      name: 'Spark Master Cluster',
      desc: 'Streaming computation engine',
      port: ':8080',
      url: 'http://localhost:8080',
      icon: Cpu,
      color: '#a855f7',
    },
  ];

  return (
    <div className="card">
      <h3 className="card-title">
        <Database size={18} color="#f59e0b" />
        Pipeline Services
      </h3>
      <p style={{ fontSize: '0.825rem', color: '#94a3b8', marginBottom: '1.25rem' }}>
        Live pipeline components powering real-time streaming and data analytics:
      </p>

      <div className="services-list">
        {services.map((svc, i) => {
          const IconComp = svc.icon;
          return (
            <a
              key={i}
              href={svc.url}
              target="_blank"
              rel="noreferrer"
              className="service-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                <IconComp size={20} color={svc.color} />
                <div className="service-info">
                  <span className="service-name">{svc.name}</span>
                  <span className="service-desc">{svc.desc}</span>
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                <span className="service-port">{svc.port}</span>
                <ExternalLink size={13} color="#64748b" />
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
}
