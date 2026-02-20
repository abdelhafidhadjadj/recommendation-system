

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email         VARCHAR(255) UNIQUE NOT NULL,
    nom           VARCHAR(255) NOT NULL,
    prenom        VARCHAR(255),
    institution   VARCHAR(500),
    -- Domaines de recherche déclarés (liste JSON)
    -- Ex: ["bioinformatics", "genomics", "deep learning"]
    domaines      JSONB DEFAULT '[]',
    mot_de_passe  VARCHAR(255) NOT NULL,  -- hashé avec bcrypt
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login    TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_domaines ON users USING GIN(domaines);

CREATE TABLE IF NOT EXISTS interactions (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id          UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    article_id       VARCHAR(255) NOT NULL,
    action_type      VARCHAR(50) NOT NULL,
    rating           FLOAT CHECK (rating >= 1 AND rating <= 5),
    duration_seconds INTEGER DEFAULT 0,
    recommended_by   VARCHAR(50),  
    timestamp        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_article ON interactions(article_id);
CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON interactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_interactions_user_article
    ON interactions(user_id, article_id, action_type);

CREATE TABLE IF NOT EXISTS sessions (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token      TEXT UNIQUE NOT NULL,
    ip_address INET,
    user_agent TEXT,
    is_active  BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revoked_at TIMESTAMP WITH TIME ZONE  -- NULL si session encore valide
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS recommendation_logs (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id        UUID REFERENCES users(id) ON DELETE SET NULL,
    query_text     TEXT,
    results_ids    JSONB NOT NULL DEFAULT '[]',
    algorithm      VARCHAR(50) NOT NULL,  -- content_based | collaborative | hybrid
    embedding_model VARCHAR(100),         -- biobert | scibert | pubmedbert
    alpha_value    FLOAT,
    results_count  INTEGER DEFAULT 0,
    latency_ms     INTEGER,
    timestamp      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reco_logs_user ON recommendation_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_reco_logs_timestamp ON recommendation_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_reco_logs_algorithm ON recommendation_logs(algorithm);


CREATE TABLE IF NOT EXISTS evaluation_results (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    experiment_name VARCHAR(255) NOT NULL,
    algorithm       VARCHAR(50) NOT NULL,
    embedding_model VARCHAR(100),
    dataset_name    VARCHAR(100),   -- pubmed | s2orc | cord19
    k_value         INTEGER,        -- K dans Précision@K, NDCG@K
    -- Métriques
    precision_at_k  FLOAT,
    recall_at_k     FLOAT,
    ndcg_at_k       FLOAT,
    f1_score        FLOAT,
    mae             FLOAT,
    rmse            FLOAT,
    -- Contexte
    alpha_value     FLOAT,          -- paramètre d'hybridation
    num_users       INTEGER,        -- nombre d'utilisateurs évalués
    num_articles    INTEGER,        -- taille du corpus
    notes           TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eval_experiment ON evaluation_results(experiment_name);
CREATE INDEX IF NOT EXISTS idx_eval_algorithm ON evaluation_results(algorithm);


CREATE TABLE IF NOT EXISTS articles_metadata (
    id               VARCHAR(255) PRIMARY KEY, 
    source           VARCHAR(50) NOT NULL,       -- pubmed | arxiv | s2orc
    title            TEXT NOT NULL,
    publication_date DATE,
    journal          VARCHAR(500),
    citations_count  INTEGER DEFAULT 0,
    is_embedded      BOOLEAN DEFAULT FALSE,      -- embedding calculé par Spark ?
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_articles_source ON articles_metadata(source);
CREATE INDEX IF NOT EXISTS idx_articles_date ON articles_metadata(publication_date DESC);
CREATE INDEX IF NOT EXISTS idx_articles_embedded ON articles_metadata(is_embedded);


CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trigger_articles_updated_at
    BEFORE UPDATE ON articles_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

INSERT INTO users (email, nom, prenom, institution, domaines, mot_de_passe)
VALUES (
    'admin@usthb.dz',
    'Admin',
    'Système',
    'USTHB - Faculté Informatique',
    '["bioinformatics", "deep learning", "genomics", "NLP"]',
    -- mot de passe : "admin123" hashé avec bcrypt (à changer en prod !)
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMeJf85MThZVlm7YKPBmX.3eWy'
) ON CONFLICT (email) DO NOTHING;


CREATE OR REPLACE VIEW user_stats AS
SELECT
    u.id,
    u.email,
    u.nom,
    COUNT(DISTINCT i.article_id) AS articles_lus,
    AVG(i.rating) AS note_moyenne,
    MAX(i.timestamp) AS derniere_activite
FROM users u
LEFT JOIN interactions i ON u.id = i.user_id
GROUP BY u.id, u.email, u.nom;

CREATE OR REPLACE VIEW popular_articles AS
SELECT
    article_id,
    COUNT(*) AS nombre_interactions,
    AVG(rating) AS note_moyenne,
    COUNT(DISTINCT user_id) AS nombre_utilisateurs
FROM interactions
WHERE action_type IN ('read', 'download', 'bookmark', 'rating')
GROUP BY article_id
ORDER BY nombre_interactions DESC;

COMMIT;