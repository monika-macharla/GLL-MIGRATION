import React, { useState, useEffect } from 'react';
import { Database, Server, User, Lock, ArrowRight, Play, CheckCircle2, XCircle, Loader2, Plus, Trash2, Settings2, Clock, RotateCcw } from 'lucide-react';

interface DBConfig {
  db_type: string;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}

interface TableMapping {
  source_table: string;
  destination_table: string;
  columns: Record<string, string>;
}

interface HistoryLog {
  id: number;
  timestamp: string;
  source_db: string;
  dest_db: string;
  source_table: string;
  dest_table: string;
  status: string;
  records: number;
  error?: string;
}

function App() {
const [source, setSource] = useState<DBConfig>({
  db_type: 'mysql',
  host: 'localhost',
  port: 3306,
  username: 'root',
  password: 'Admin!1',
  database: 'dcccd'
});

  const [dest, setDest] = useState<DBConfig>({
    db_type: 'mysql',
    host: 'localhost',
    port: 3306,
    username: 'root',
    password: 'Admin!1',
    database: 'gllauthservicemigration'
  });

  const [sourceSchema, setSourceSchema] = useState<Record<string, string[]>>({});
  const [destSchema, setDestSchema] = useState<Record<string, string[]>>({});
  const [mappings, setMappings] = useState<TableMapping[]>([]);
  const [history, setHistory] = useState<HistoryLog[]>([]);

  const [status, setStatus] = useState<{ type: 'idle' | 'loading' | 'success' | 'error', message: string }>({
    type: 'idle',
    message: ''
  });

  const [testResults, setTestResults] = useState<{ source: string, dest: string }>({
    source: '',
    dest: ''
  });
  const [limit, setLimit] = useState<number | undefined>(undefined);

  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    try {
      const response = await fetch('http://localhost:8000/history');
      const data = await response.json();
      setHistory(data);
    } catch (err) {
      console.error("Failed to fetch history", err);
    }
  };

  const fetchSchema = async (type: 'source' | 'dest') => {
    const config = type === 'source' ? source : dest;
    try {
      const response = await fetch('http://localhost:8000/schema', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });
      const data = await response.json();
      if (data.status === 'success') {
        if (type === 'source') setSourceSchema(data.schema);
        else setDestSchema(data.schema);
      }
    } catch (err) {
      console.error("Failed to fetch schema", err);
    }
  };

  const handleTestConnection = async (type: 'source' | 'dest') => {
    const config = type === 'source' ? source : dest;
    setTestResults(prev => ({ ...prev, [type]: 'Testing...' }));
    try {
      const response = await fetch('http://localhost:8000/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });
      const data = await response.json();
      const success = data.status === 'success';
      setTestResults(prev => ({ ...prev, [type]: success ? 'Connected' : 'Failed' }));
      if (success) {
        fetchSchema(type);
      }
    } catch (err) {
      setTestResults(prev => ({ ...prev, [type]: 'Error' }));
    }
  };

  const handleMigrate = async () => {
    if (mappings.length === 0) {
      setStatus({ type: 'error', message: 'Please add at least one table mapping' });
      return;
    }

    setStatus({ type: 'loading', message: 'Starting migration...' });
    try {
      const response = await fetch('http://localhost:8000/migrate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source,
          destination: dest,
          mappings,
          limit
        })
      });
      const data = await response.json();
      if (response.ok) {
        setStatus({ type: 'success', message: 'Migration completed successfully!' });
        fetchHistory();
      } else {
        setStatus({ type: 'error', message: data.detail || 'Migration failed' });
        fetchHistory();
      }
    } catch (err) {
      setStatus({ type: 'error', message: 'Connection to backend failed' });
    }
  };

  const addMapping = () => {
    setMappings([...mappings, { source_table: '', destination_table: '', columns: {} }]);
  };

  const updateMapping = (index: number, field: keyof TableMapping, value: any) => {
    const newMappings = [...mappings];
    newMappings[index] = { ...newMappings[index], [field]: value };
    // Auto-fill destination table if same name and user wants to create if not exists
    if (field === 'source_table') {
      if (value === 'institution' || value === 'address') {
        newMappings[index].destination_table = 'institutions';
      } else if (!newMappings[index].destination_table) {
        newMappings[index].destination_table = value;
      }
    }
    setMappings(newMappings);
  };

  const addColumnMapping = (tableIndex: number, srcCol: string, destCol: string) => {
    const newMappings = [...mappings];
    newMappings[tableIndex].columns = { ...newMappings[tableIndex].columns, [srcCol]: destCol };
    setMappings(newMappings);
  };

  const removeTableMapping = (index: number) => {
    setMappings(mappings.filter((_, i) => i !== index));
  };

  const renderConfigForm = (title: string, config: DBConfig, setConfig: React.Dispatch<React.SetStateAction<DBConfig>>, type: 'source' | 'dest') => (
    <div className="card">
      <h2 className="card-title">
        <Database size={20} color="#a855f7" />
        {title}
      </h2>

      <div className="grid" style={{ gridTemplateColumns: '3fr 1fr', gap: '1rem', marginBottom: 0 }}>
        <div className="form-group">
          <label>Host</label>
          <input type="text" value={config.host} onChange={e => setConfig({ ...config, host: e.target.value })} placeholder="localhost" />
        </div>
        <div className="form-group">
          <label>Port</label>
          <input type="number" value={config.port} onChange={e => setConfig({ ...config, port: parseInt(e.target.value) })} />
        </div>
      </div>

      <div className="form-group">
        <label>Username</label>
        <input type="text" value={config.username} onChange={e => setConfig({ ...config, username: e.target.value })} />
      </div>

      <div className="form-group">
        <label>Password</label>
        <input type="password" value={config.password} onChange={e => setConfig({ ...config, password: e.target.value })} />
      </div>

      <div className="form-group">
        <label>Database Name</label>
        <input type="text" value={config.database} onChange={e => setConfig({ ...config, database: e.target.value })} placeholder="mydb" />
      </div>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: '1rem' }}>
        <button className="btn btn-secondary" onClick={() => handleTestConnection(type)}>
          Test Connection
        </button>
        {testResults[type] && (
          <span className={`status-badge ${testResults[type] === 'Connected' ? 'status-success' : (testResults[type] === 'Testing...' ? 'status-idle' : 'status-error')}`}>
            {testResults[type]}
          </span>
        )}
      </div>
    </div>
  );

  return (
    <div className="app-container">
      <header>
        <h1>Antigravity Migrate</h1>
        <p>GLL Project Data Migration Control Center</p>
      </header>

      <div className="grid">
        {renderConfigForm("Source Database", source, setSource, 'source')}
        {renderConfigForm("Destination Database", dest, setDest, 'dest')}
      </div>

      <div className="grid" style={{ gridTemplateColumns: '250px 1fr', gap: '2rem', alignItems: 'start' }}>
        {/* Sidebar for Source Tables */}
        <div className="card" style={{ position: 'sticky', top: '2rem', minHeight: '400px' }}>
          <h2 className="card-title" style={{ fontSize: '1rem' }}>
            <Database size={16} color="#a855f7" />
            Source Tables
          </h2>
          {Object.keys(sourceSchema).length === 0 ? (
            <p style={{ fontSize: '0.8rem', color: 'hsl(var(--muted-foreground))' }}>
              Connect to source database to view tables.
            </p>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {Object.keys(sourceSchema).map(table => (
                <button
                  key={table}
                  className="btn btn-secondary"
                  style={{ justifyContent: 'flex-start', fontSize: '0.85rem', padding: '0.5rem 0.75rem', width: '100%' }}
                  onClick={() => {
                    if (!mappings.find(m => m.source_table === table)) {
                      setMappings([...mappings, { source_table: table, destination_table: table, columns: {} }]);
                    }
                  }}
                >
                  <Plus size={14} />
                  {table}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Main Mapping Area */}
        <div>
          <div className="card" style={{ marginBottom: '2rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
              <h2 className="card-title" style={{ margin: 0 }}>
                <Settings2 size={20} color="#a855f7" />
                Mapping Configuration
              </h2>
              <button className="btn btn-secondary" onClick={addMapping} disabled={!testResults.source || !testResults.dest}>
                <Plus size={18} />
                Add Custom Mapping
              </button>
            </div>

            {mappings.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '3rem', color: 'hsl(var(--muted-foreground))', background: 'hsla(var(--muted), 0.3)', borderRadius: 'var(--radius)' }}>
                <p>Select tables from the sidebar or click "Add Custom Mapping" to begin.</p>
              </div>
            ) : (
              mappings.map((mapping, idx) => {
                const isGLL = ['institutions', 'institution_campuses'].includes(mapping.destination_table);
                return (
                  <div key={idx} style={{ marginBottom: '2rem', padding: '1.5rem', background: 'hsla(var(--muted), 0.3)', borderRadius: 'var(--radius)', border: '1px solid hsl(var(--border))' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                      <span className={`status-badge ${isGLL ? 'status-success' : 'status-idle'}`} style={{ fontSize: '0.65rem' }}>
                        {isGLL ? '⚡ CUSTOM GLL LOGIC DETECTED' : '📦 STANDARD MIGRATION'}
                      </span>
                    </div>
                    <div className="grid" style={{ gridTemplateColumns: '1fr 1fr 40px', alignItems: 'end', gap: '1rem' }}>
                      <div className="form-group" style={{ marginBottom: 0 }}>
                        <label>Source Table</label>
                        <select value={mapping.source_table} onChange={e => updateMapping(idx, 'source_table', e.target.value)}>
                          <option value="">Select Table</option>
                          {Object.keys(sourceSchema).map(t => <option key={t} value={t}>{t}</option>)}
                        </select>
                      </div>
                      <div className="form-group" style={{ marginBottom: 0 }}>
                        <label>Destination Table</label>
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                          <div style={{ position: 'relative', flex: 1 }}>
                            <input
                              type="text"
                              value={mapping.destination_table}
                              onChange={e => updateMapping(idx, 'destination_table', e.target.value)}
                              placeholder="Type target table..."
                              style={{ width: '100%' }}
                            />
                            <select
                              value=""
                              onChange={e => updateMapping(idx, 'destination_table', e.target.value)}
                              style={{
                                position: 'absolute',
                                right: 0,
                                top: 0,
                                width: '30px',
                                opacity: 0,
                                cursor: 'pointer'
                              }}
                            >
                              <option value="">Quick Select</option>
                              <optgroup label="GLL Targets">
                                <option value="institutions">institutions (Main)</option>
                                <option value="institution_campuses">institution_campuses</option>
                              </optgroup>
                              <optgroup label="Detected Tables">
                                {Object.keys(destSchema).map(t => <option key={t} value={t}>{t}</option>)}
                              </optgroup>
                              {mapping.source_table && <option value={mapping.source_table}>Use Source Name: {mapping.source_table}</option>}
                            </select>
                          </div>
                        </div>
                      </div>
                      <button className="btn btn-secondary" style={{ padding: '0.75rem', color: '#ef4444' }} onClick={() => removeTableMapping(idx)}>
                        <Trash2 size={18} />
                      </button>
                    </div>

                    {mapping.source_table && sourceSchema[mapping.source_table] && (
                      <div style={{ marginTop: '1rem', padding: '0.75rem', background: 'hsla(var(--muted), 0.5)', borderRadius: 'calc(var(--radius) - 2px)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                          <p style={{ fontSize: '0.75rem', color: 'hsl(var(--muted-foreground))', fontWeight: 600, margin: 0 }}>
                            {isGLL ? 'GLL CUSTOM MAPPING ACTIVE' : 'COLUMNS TO MIGRATE (AUTO-MAPPING ALL)'}
                          </p>
                          <span style={{ fontSize: '0.65rem', color: 'hsl(var(--muted-foreground))' }}>
                            {sourceSchema[mapping.source_table].length} columns found
                          </span>
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
                          {sourceSchema[mapping.source_table].map(col => (
                            <span key={col} style={{ fontSize: '0.65rem', padding: '0.15rem 0.4rem', background: 'hsl(var(--background))', border: '1px solid hsl(var(--border))', borderRadius: '4px', color: 'hsl(var(--foreground))' }}>
                              {col}
                            </span>
                          ))}
                          {sourceSchema[mapping.source_table].length === 0 && (
                            <span style={{ fontSize: '0.7rem', color: '#ef4444' }}>Warning: No columns detected in source table!</span>
                          )}
                        </div>
                      </div>
                    )}


                  </div>
                )
              })
            )}
          </div>

          <div className="actions" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', width: '100%', maxWidth: '400px' }}>
              <div className="form-group" style={{ marginBottom: 0, flex: 1 }}>
                <label style={{ fontSize: '0.7rem' }}>Migration Limit (Optional)</label>
                <input
                  type="number"
                  placeholder="All records"
                  onChange={e => setLimit(e.target.value ? parseInt(e.target.value) : undefined)}
                  style={{ height: '40px' }}
                />
              </div>
              <button
                className="btn btn-primary"
                style={{ height: '40px', marginTop: '1.2rem', flex: 2 }}
                onClick={handleMigrate}
                disabled={status.type === 'loading' || testResults.source !== 'Connected' || testResults.dest !== 'Connected'}
              >
                {status.type === 'loading' ? (
                  <Loader2 className="animate-spin" />
                ) : (
                  <Play size={18} fill="currentColor" />
                )}
                Run Migration
              </button>
            </div>
            {(testResults.source !== 'Connected' || testResults.dest !== 'Connected') && (
              <p style={{ marginTop: '1rem', color: '#f59e0b', fontSize: '0.9rem', fontWeight: 500, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.5rem' }}>
                <Settings2 size={16} />
                Please test and connect both databases to enable migration.
              </p>
            )}
            {status.type !== 'idle' && (
              <div style={{ marginTop: '1rem', textAlign: 'center' }}>
                <div className={`status-badge ${status.type === 'success' ? 'status-success' : 'status-error'}`} style={{ fontSize: '1rem', padding: '0.5rem 1.5rem', display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}>
                  {status.type === 'success' ? <CheckCircle2 size={18} /> : <XCircle size={18} />}
                  <span>{status.message}</span>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="card" style={{ marginTop: '3rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
          <h2 className="card-title" style={{ margin: 0 }}>
            <Clock size={20} color="#a855f7" />
            Migration History
          </h2>
          <button className="btn btn-secondary" onClick={fetchHistory}>
            <RotateCcw size={16} />
            Refresh
          </button>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid hsl(var(--border))', color: 'hsl(var(--muted-foreground))' }}>
                <th style={{ textAlign: 'left', padding: '1rem' }}>Time</th>
                <th style={{ textAlign: 'left', padding: '1rem' }}>Source Table</th>
                <th style={{ textAlign: 'left', padding: '1rem' }}>Target Table</th>
                <th style={{ textAlign: 'left', padding: '1rem' }}>Status</th>
                <th style={{ textAlign: 'left', padding: '1rem' }}>Records</th>
              </tr>
            </thead>
            <tbody>
              {history.length === 0 ? (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '2rem', color: 'hsl(var(--muted-foreground))' }}>No history found</td>
                </tr>
              ) : (
                history.map(log => (
                  <tr key={log.id} style={{ borderBottom: '1px solid hsla(var(--border), 0.5)' }}>
                    <td style={{ padding: '1rem' }}>{new Date(log.timestamp).toLocaleString()}</td>
                    <td style={{ padding: '1rem' }}>{log.source_table}</td>
                    <td style={{ padding: '1rem' }}>{log.dest_table}</td>
                    <td style={{ padding: '1rem' }}>
                      <span className={`status-badge ${log.status === 'success' ? 'status-success' : 'status-error'}`} style={{ fontSize: '0.7rem' }}>
                        {log.status}
                      </span>
                    </td>
                    <td style={{ padding: '1rem' }}>{log.records}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default App;
