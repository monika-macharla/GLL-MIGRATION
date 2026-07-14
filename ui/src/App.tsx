import { useState, useEffect, useCallback, type Dispatch, type SetStateAction } from 'react';
import { Database, Play, CheckCircle2, XCircle, Loader2, Plus, Trash2, Settings2, Clock, RotateCcw } from 'lucide-react';

interface DBConfig {
  db_type: string;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}

interface LookupDBConfig extends DBConfig {
  name: string;
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

interface TestResults {
  source: string;
  dest: string;
  lookup: Record<number, string>;
}

const API_BASE_URL = window.location.protocol + '//' + window.location.hostname + ':8000';

function App() {
const [source, setSource] = useState<DBConfig>({
  db_type: 'mysql',
  host: 'gll-production.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com',
  port: 3306,
  username: 'gll_prod',
  password: 'G11XlpM0c6202',
  database: 'greenlight'
});

  const [dest, setDest] = useState<DBConfig>({
    db_type: 'mysql',
    host: 'gll-production.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com',
    port: 3306,
    username: 'gll_prod',
    password: 'G11XlpM0c6202',
    database: 'gllauthservice',
    // database: 'gllreports'
    // database: 'glldataingestion'
  });

  const [lookupDatabases, setLookupDatabases] = useState<LookupDBConfig[]>([
  {
    name: 'auth_db',
    db_type: 'mysql',
    host: 'gll-production.cx4i0k8o6lzg.us-east-2.rds.amazonaws.com',
    port: 3306,
    username: 'gll_prod',
    password: 'G11XlpM0c6202',
    database: 'gllauthservice'
  }
]);

  const [sourceSchema, setSourceSchema] = useState<Record<string, string[]>>({});
  const [destSchema, setDestSchema] = useState<Record<string, string[]>>({});
  const [mappings, setMappings] = useState<TableMapping[]>([]);
  const [history, setHistory] = useState<HistoryLog[]>([]);

  const [status, setStatus] = useState<{ type: 'idle' | 'loading' | 'success' | 'error', message: string }>({
    type: 'idle',
    message: ''
  });

  const [testResults, setTestResults] = useState<TestResults>({
  source: '',
  dest: '',
  lookup: {}
});
  const [limit, setLimit] = useState<number | undefined>(undefined);

  const fetchHistory = useCallback(async () => {
    try {
      const response = await fetch(API_BASE_URL + '/history');
      const data = await response.json();
      setHistory(data);
    } catch (err) {
      console.error("Failed to fetch history", err);
    }
  }, []);

  useEffect(() => {
    void Promise.resolve().then(fetchHistory);
  }, [fetchHistory]);

  const fetchSchema = async (type: 'source' | 'dest') => {
    const config = type === 'source' ? source : dest;
    try {
      const response = await fetch(API_BASE_URL + '/schema', {
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
      const response = await fetch(API_BASE_URL + '/test-connection', {
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
    } catch {
      setTestResults(prev => ({ ...prev, [type]: 'Error' }));
    }
  };

  const handleLookupTestConnection = async (
  lookup: LookupDBConfig,
  index: number
) => {

  setTestResults((prev) => ({
    ...prev,
    lookup: {
      ...prev.lookup,
      [index]: 'Testing...'
    }
  }));

  try {

    const response = await fetch(
      API_BASE_URL + '/test-connection',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(lookup)
      }
    );

    const data = await response.json();

    const success =
      data.status === 'success';

    setTestResults((prev) => ({
      ...prev,
      lookup: {
        ...prev.lookup,
        [index]: success
          ? 'Connected'
          : 'Failed'
      }
    }));

  } catch {

    setTestResults((prev) => ({
      ...prev,
      lookup: {
        ...prev.lookup,
        [index]: 'Error'
      }
    }));
  }
};

  const handleMigrate = async () => {
    if (mappings.length === 0) {
      setStatus({ type: 'error', message: 'Please add at least one table mapping' });
      return;
    }

    setStatus({ type: 'loading', message: 'Starting migration...' });
    try {
      const response = await fetch(API_BASE_URL + '/migrate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source,
          destination: dest,
          mappings,
          lookup_databases: lookupDatabases,
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
    } catch {
      setStatus({ type: 'error', message: 'Connection to backend failed' });
    }
  };

  const addMapping = () => {
    setMappings([...mappings, { source_table: '', destination_table: '', columns: {} }]);
  };

  const updateMapping = <K extends keyof TableMapping>(
    index: number,
    field: K,
    value: TableMapping[K]
  ) => {
    setMappings(prevMappings => prevMappings.map((mapping, mappingIndex) => {
      if (mappingIndex !== index) {
        return mapping;
      }

      const updatedMapping = { ...mapping, [field]: value };

      if (field !== 'source_table') {
        return updatedMapping;
      }

      const sourceTable = value as TableMapping['source_table'];

      if (sourceTable === 'institution' || sourceTable === 'address') {
        return { ...updatedMapping, destination_table: 'institutions' };
      }

      if (sourceTable === 'cc_degree_awarded' || sourceTable === 'cc_degree_awaeded') {
        return { ...updatedMapping, destination_table: 'import_edi_award' };
      }

      if (sourceTable === 'cc_courses' || sourceTable === 'cc_course') {
        return { ...updatedMapping, destination_table: 'import_edi_courses' };
      }

      if (sourceTable === 'cc_term') {
        return { ...updatedMapping, destination_table: 'import_edi_semester' };
      }

      if (sourceTable === 'cc_external_articulated_registration') {
        return { ...updatedMapping, destination_table: 'import_edi_external_articulated_registration' };
      }

      if (sourceTable === 'cc_transfer_credit_summary') {
        return { ...updatedMapping, destination_table: 'import_edi_institutions_attended' };
      }

      if (sourceTable === 'cc_transctript_ext') {
        return { ...updatedMapping, destination_table: 'import_edi_gpa' };
      }

      if (sourceTable === 'cc_transcript_ext' || sourceTable === 'cc_transcript_extended_info') {
        return { ...updatedMapping, destination_table: 'import_edi_transcript_ext' };
      }

      if (!mapping.destination_table) {
        return { ...updatedMapping, destination_table: sourceTable };
      }

      return updatedMapping;
    }));
  };

  const removeTableMapping = (index: number) => {
    setMappings(mappings.filter((_, i) => i !== index));
  };

  const renderConfigForm = (title: string, config: DBConfig, setConfig: Dispatch<SetStateAction<DBConfig>>, type: 'source' | 'dest') => (
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
        <div className="app-title">Antigravity Migrate</div>
        <p>GLL Project Data Migration Control Center</p>
      </header>

      <div className="grid">
        {renderConfigForm("Source Database", source, setSource, 'source')}
        {renderConfigForm("Destination Database", dest, setDest, 'dest')}
      </div>

      <div
  className="card"
  style={{ marginTop: '2rem' }}
>

  <div className="card-title">
    <Database size={20} color="#22c55e" />
    Lookup Databases
  </div>

  {lookupDatabases.map((lookup, index) => (

    <div
      key={index}
      style={{
        marginBottom: '2rem',
        padding: '1rem',
        border: '1px solid hsl(var(--border))',
        borderRadius: 'var(--radius)'
      }}
    >

      <div className="form-group">
        <label>Lookup Name</label>

        <input
          type="text"
          value={lookup.name}
          onChange={(e) => {

            const updated = [...lookupDatabases];

            updated[index].name =
              e.target.value;

            setLookupDatabases(updated);
          }}
        />
      </div>

      <div
        className="grid"
        style={{
          gridTemplateColumns: '3fr 1fr',
          gap: '1rem'
        }}
      >

        <div className="form-group">
          <label>Host</label>

          <input
            type="text"
            value={lookup.host}
            onChange={(e) => {

              const updated = [...lookupDatabases];

              updated[index].host =
                e.target.value;

              setLookupDatabases(updated);
            }}
          />
        </div>

        <div className="form-group">
          <label>Port</label>

          <input
            type="number"
            value={lookup.port}
            onChange={(e) => {

              const updated = [...lookupDatabases];

              updated[index].port =
                parseInt(e.target.value);

              setLookupDatabases(updated);
            }}
          />
        </div>
      </div>

      <div className="form-group">
        <label>Username</label>

        <input
          type="text"
          value={lookup.username}
          onChange={(e) => {

            const updated = [...lookupDatabases];

            updated[index].username =
              e.target.value;

            setLookupDatabases(updated);
          }}
        />
      </div>

      <div className="form-group">
        <label>Password</label>

        <input
          type="password"
          value={lookup.password}
          onChange={(e) => {

            const updated = [...lookupDatabases];

            updated[index].password =
              e.target.value;

            setLookupDatabases(updated);
          }}
        />
      </div>

      <div className="form-group">
        <label>Database Name</label>

        <input
          type="text"
          value={lookup.database}
          onChange={(e) => {

            const updated = [...lookupDatabases];

            updated[index].database =
              e.target.value;

            setLookupDatabases(updated);
          }}
        />
      </div>

      <button
        className="btn btn-secondary"
        onClick={() =>
          handleLookupTestConnection(
            lookup,
            index
          )
        }
      >
        Test Lookup Connection
      </button>

      {testResults.lookup[index] && (

        <span
          className={`status-badge ${
            testResults.lookup[index]
            === 'Connected'

            ? 'status-success'

            : 'status-error'
          }`}
          style={{ marginLeft: '1rem' }}
        >
          {testResults.lookup[index]}
        </span>
      )}
    </div>
  ))}

  <button
    className="btn btn-primary"
    onClick={() => {

      setLookupDatabases([

        ...lookupDatabases,

        {
          name: '',
          db_type: 'mysql',
          host: '',
          port: 3306,
          username: '',
          password: '',
          database: ''
        }
      ]);
    }}
  >
    <Plus size={16} />
    Add Lookup DB
  </button>
</div>

      <div className="grid" style={{ gridTemplateColumns: '250px 1fr', gap: '2rem', alignItems: 'start' }}>
        {/* Sidebar for Source Tables */}
        <div className="card" style={{ position: 'sticky', top: '2rem', minHeight: '400px' }}>
          <div className="card-title" style={{ fontSize: '1rem' }}>
            <Database size={16} color="#a855f7" />
            Source Tables
          </div>
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
              <div className="card-title" style={{ margin: 0 }}>
                <Settings2 size={20} color="#a855f7" />
                Mapping Configuration
              </div>
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
                const isGLL = [
                  'institutions',
                  'institution_campuses',
                  'user_hashed_password_expires_at'
                ].includes(mapping.destination_table);
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
                                <option value="user_hashed_password_expires_at">password expires_at fix</option>
                                <option value="import_edi_external_articulated_registration">import_edi_external_articulated_registration</option>
                                <option value="import_edi_institutions_attended">import_edi_institutions_attended</option>
                                <option value="import_edi_gpa">import_edi_gpa</option>
                                <option value="import_edi_transcript_ext">import_edi_transcript_ext</option>
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
                disabled={status.type === 'loading' || testResults.source !== 'Connected' || testResults.dest !== 'Connected'||Object.values(testResults.lookup || {})
    .some(v => v !== 'Connected')}
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
          <div className="card-title" style={{ margin: 0 }}>
            <Clock size={20} color="#a855f7" />
            Migration History
          </div>
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
