-- Table for storing comprehensive table analysis results
CREATE TABLE IF NOT EXISTS dsa_core.table_analysis (
    commit_id TEXT NOT NULL,
    table_key TEXT NOT NULL,
    analysis JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (commit_id, table_key),
    FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE
);

-- Index for faster lookups
CREATE INDEX idx_table_analysis_commit_id ON dsa_core.table_analysis(commit_id);
CREATE INDEX idx_table_analysis_table_key ON dsa_core.table_analysis(table_key);