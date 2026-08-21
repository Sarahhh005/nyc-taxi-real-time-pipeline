import React, { useState } from 'react';
import { Copy, Check, Sparkles, Clock } from 'lucide-react';

export default function ResponseCard({ item }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(item.answer);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  /**
   * Render formatted text for AI responses:
   * Formats lists, bullet points, bold text, numbers, and code snippets nicely.
   */
  const formatText = (text) => {
    if (!text) return null;

    const lines = text.split('\n');
    return lines.map((line, index) => {
      // Code blocks or SQL queries
      if (line.startsWith('SELECT') || line.startsWith('SELECT') || line.startsWith('SQL:')) {
        return (
          <pre key={index} className="code-block">
            <code>{line}</code>
          </pre>
        );
      }

      // Bullet points
      if (line.trim().startsWith('- ') || line.trim().startsWith('* ')) {
        const content = line.trim().substring(2);
        return (
          <li key={index} style={{ marginLeft: '1rem', marginBottom: '0.35rem' }}>
            {parseBoldText(content)}
          </li>
        );
      }

      // Headers
      if (line.startsWith('### ') || line.startsWith('## ')) {
        return (
          <h4 key={index} style={{ color: '#fbbf24', marginTop: '0.85rem', marginBottom: '0.35rem', fontWeight: 600 }}>
            {line.replace(/#+\s*/, '')}
          </h4>
        );
      }

      // Regular paragraph
      if (line.trim() === '') {
        return <div key={index} style={{ height: '0.5rem' }} />;
      }

      return (
        <p key={index} style={{ marginBottom: '0.4rem' }}>
          {parseBoldText(line)}
        </p>
      );
    });
  };

  // Helper to parse **bold** text inline
  const parseBoldText = (str) => {
    const parts = str.split(/(\*\*.*?\*\*)/g);
    return parts.map((part, idx) => {
      if (part.startsWith('**') && part.endsWith('**')) {
        return <strong key={idx} style={{ color: '#f8fafc' }}>{part.slice(2, -2)}</strong>;
      }
      return part;
    });
  };

  return (
    <div className="history-item">
      <div className="item-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <Sparkles size={16} color="#fbbf24" />
          <span className="user-question">{item.question}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span className="item-timestamp" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <Clock size={12} />
            {item.timestamp}
          </span>
          <button
            onClick={handleCopy}
            className="btn btn-secondary"
            style={{ padding: '0.25rem 0.6rem', fontSize: '0.75rem' }}
            title="Copy answer"
          >
            {copied ? <Check size={13} color="#10b981" /> : <Copy size={13} />}
            {copied ? 'Copied' : 'Copy'}
          </button>
        </div>
      </div>

      <div className="response-content">
        {formatText(item.answer)}
      </div>
    </div>
  );
}
