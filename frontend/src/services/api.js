const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/**
 * Check backend health status
 */
export async function checkHealth() {
  try {
    const response = await fetch(`${BASE_URL}/health`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
    });
    if (!response.ok) return { status: 'offline' };
    const data = await response.json();
    return { status: 'online', data };
  } catch (error) {
    return { status: 'offline', error: error.message };
  }
}

/**
 * Send question and stream response text chunks in real-time
 */
export async function askQuestionStream(question, onChunk) {
  if (!question || !question.trim()) {
    throw new Error('Please enter a valid question.');
  }

  try {
    const response = await fetch(`${BASE_URL}/ask/stream`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ question: question.trim() }),
    });

    if (!response.ok) {
      throw new Error(`Server returned status ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let accumulatedText = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      accumulatedText += chunk;
      if (onChunk) {
        onChunk(accumulatedText);
      }
    }

    return accumulatedText;
  } catch (err) {
    // Fallback to standard /ask if streaming endpoint is unavailable
    console.warn('Streaming failed, falling back to standard /ask:', err.message);
    const standardRes = await askQuestion(question);
    if (onChunk) onChunk(standardRes.answer);
    return standardRes.answer;
  }
}

/**
 * Send natural language question to AI Agent (standard full payload)
 */
export async function askQuestion(question) {
  if (!question || !question.trim()) {
    throw new Error('Please enter a valid question.');
  }

  const response = await fetch(`${BASE_URL}/ask`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
    body: JSON.stringify({ question: question.trim() }),
  });

  if (!response.ok) {
    let errorDetail = 'Failed to get response from AI Agent.';
    try {
      const errData = await response.json();
      errorDetail = errData.detail || errData.message || errorDetail;
    } catch (_) {
      // Use fallback
    }
    throw new Error(errorDetail);
  }

  const data = await response.json();
  return data;
}
