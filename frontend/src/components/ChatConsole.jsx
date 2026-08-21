import React, { useState } from 'react';
import { Send, Trash2, Bot, AlertTriangle, Sparkles } from 'lucide-react';
import ResponseCard from './ResponseCard';
import { askQuestionStream } from '../services/api';

export default function ChatConsole({ backendStatus }) {
  const [question, setQuestion] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState([
    {
      question: 'System Status & Initial Welcome',
      answer: 'Welcome to the NYC Taxi AI Analytics Console! Select a quick preset prompt below or type any custom query to analyze trip counts, revenue, peak demand hours, and location statistics from live ClickHouse data.',
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    },
  ]);

  const presetQuestions = [
    'How many total trips are in the database?',
    'What is the peak hour for trip demand?',
    'Show revenue summary and average fare',
    'Which pickup location has the highest trip count?',
    'Are there any trip anomalies or outliers?',
    'What is the average trip distance and median?',
  ];

  const handleSubmit = async (e) => {
    if (e) e.preventDefault();
    if (!question.trim() || loading) return;

    const currentQuestion = question.trim();
    setQuestion('');
    setError(null);
    setLoading(true);

    const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    
    // Add placeholder item to top of history
    const initialItem = {
      question: currentQuestion,
      answer: 'Thinking...',
      timestamp,
    };
    
    setHistory((prev) => [initialItem, ...prev]);

    try {
      await askQuestionStream(currentQuestion, (accumulatedText) => {
        setHistory((prev) => {
          const updated = [...prev];
          if (updated.length > 0) {
            updated[0] = { ...updated[0], answer: accumulatedText };
          }
          return updated;
        });
      });
    } catch (err) {
      setError(err.message || 'Failed to communicate with AI Agent.');
    } finally {
      setLoading(false);
    }
  };

  const handlePresetClick = (qText) => {
    setQuestion(qText);
  };

  const handleClearHistory = () => {
    setHistory([]);
    setError(null);
  };

  return (
    <div className="card" style={{ display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <h2 className="card-title" style={{ margin: 0 }}>
          <Bot size={22} color="#fbbf24" />
          AI Analytics Assistant
        </h2>
        {history.length > 0 && (
          <button
            onClick={handleClearHistory}
            className="btn btn-secondary"
            style={{ padding: '0.35rem 0.75rem', fontSize: '0.8rem' }}
          >
            <Trash2 size={14} /> Clear History
          </button>
        )}
      </div>

      {/* Preset Prompts */}
      <div className="presets-section">
        <div className="presets-label">Quick Prompts</div>
        <div className="presets-grid">
          {presetQuestions.map((q, idx) => (
            <button
              key={idx}
              type="button"
              className="preset-chip"
              onClick={() => handlePresetClick(q)}
            >
              <Sparkles size={12} color="#f59e0b" style={{ display: 'inline', marginRight: '4px' }} />
              {q}
            </button>
          ))}
        </div>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="error-banner">
          <AlertTriangle size={18} />
          <span>{error}</span>
        </div>
      )}

      {/* Input Form */}
      <form onSubmit={handleSubmit} className="query-form">
        <div className="input-wrapper">
          <input
            type="text"
            className="query-input"
            placeholder="Ask anything about NYC Taxi trips (e.g. revenue, peak hours, top zones)..."
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            disabled={loading}
          />
        </div>
        <div className="form-actions">
          <button
            type="submit"
            className="btn btn-primary"
            disabled={loading || !question.trim()}
          >
            <Send size={16} />
            {loading ? 'Streaming Response...' : 'Ask AI Agent'}
          </button>
        </div>
      </form>

      {/* Response History */}
      <div className="history-container">
        {history.length === 0 && !loading ? (
          <div className="empty-state">
            <div className="empty-icon">🚖</div>
            <p>No queries yet. Click a quick prompt above or type your question!</p>
          </div>
        ) : (
          history.map((item, idx) => (
            <ResponseCard key={idx} item={item} />
          ))
        )}
      </div>
    </div>
  );
}
