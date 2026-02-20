// ============================================================
// INITIALISATION MONGODB
// Système de recommandation d'articles scientifiques
// USTHB — PFE Master Bioinformatique 2025/2026
// ============================================================

// Se connecter à la base de données du projet
db = db.getSiblingDB('recommender_articles');

// ============================================================
// COLLECTION : articles_raw
// Articles bruts tels que reçus depuis les sources
// Pas de transformation — archive fidèle des données sources
// ============================================================
db.createCollection('articles_raw', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['id', 'source', 'title', 'collected_at'],
            properties: {
                id: {
                    bsonType: 'string',
                    description: 'Identifiant unique — ex: pubmed_12345678'
                },
                source: {
                    bsonType: 'string',
                    enum: ['pubmed', 'arxiv', 's2orc', 'manual'],
                    description: 'Source de collecte de l article'
                },
                title: {
                    bsonType: 'string',
                    description: 'Titre de l article'
                },
                abstract: {
                    bsonType: 'string',
                    description: 'Résumé de l article'
                },
                authors: {
                    bsonType: 'array',
                    description: 'Liste des auteurs'
                },
                publication_date: {
                    bsonType: 'string',
                    description: 'Date de publication format ISO'
                },
                doi: {
                    bsonType: 'string',
                    description: 'Digital Object Identifier'
                },
                keywords: {
                    bsonType: 'array',
                    description: 'Mots-clés fournis par l article'
                },
                journal: {
                    bsonType: 'string',
                    description: 'Nom de la revue ou conférence'
                },
                citations_count: {
                    bsonType: 'int',
                    description: 'Nombre de citations'
                },
                references: {
                    bsonType: 'array',
                    description: 'IDs des articles référencés'
                },
                // Champs spécifiques PubMed
                pmid: { bsonType: 'string' },
                mesh_terms: { bsonType: 'array' },
                // Champs spécifiques arXiv
                arxiv_id: { bsonType: 'string' },
                arxiv_categories: { bsonType: 'array' },
                collected_at: {
                    bsonType: 'date',
                    description: 'Date de collecte par notre système'
                },
                // Statut dans le pipeline
                processing_status: {
                    bsonType: 'string',
                    enum: ['raw', 'silver', 'gold', 'error'],
                    description: 'Étape atteinte dans le pipeline Spark'
                }
            }
        }
    },
    validationAction: 'warn'  // warn = log les erreurs sans bloquer
});

// Index sur l'ID unique (évite les doublons)
db.articles_raw.createIndex({ id: 1 }, { unique: true });
// Index sur la source (pour filtrer par PubMed ou arXiv)
db.articles_raw.createIndex({ source: 1 });
// Index temporel (pour récupérer les articles récents)
db.articles_raw.createIndex({ collected_at: -1 });
// Index sur le statut pipeline (pour Spark : "quels articles pas encore traités ?")
db.articles_raw.createIndex({ processing_status: 1 });
// Index full-text sur titre et abstract (recherche textuelle basique)
db.articles_raw.createIndex(
    { title: 'text', abstract: 'text', keywords: 'text' },
    { name: 'text_search_index', weights: { title: 3, keywords: 2, abstract: 1 } }
);

// ============================================================
// COLLECTION : articles_enriched
// Articles après traitement Spark (Silver + Gold)
// Contient les métadonnées nettoyées ET les embeddings
// ============================================================
db.createCollection('articles_enriched', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['id', 'source', 'title_clean', 'abstract_clean'],
            properties: {
                id: { bsonType: 'string' },
                source: { bsonType: 'string' },
                // Texte nettoyé (Silver)
                title_clean: { bsonType: 'string' },
                abstract_clean: { bsonType: 'string' },
                text_combined: {
                    bsonType: 'string',
                    description: 'titre + [SEP] + abstract pour BioBERT'
                },
                // Métadonnées structurées
                authors: { bsonType: 'array' },
                keywords: { bsonType: 'array' },
                // Mots-clés extraits automatiquement par Spark (KeyBERT)
                keywords_extracted: { bsonType: 'array' },
                publication_date: { bsonType: 'date' },
                journal: { bsonType: 'string' },
                citations_count: { bsonType: 'int' },
                language: { bsonType: 'string' },
                domain: { bsonType: 'string' },
                // Embeddings calculés par BioBERT/SciBERT
                // Note : le vecteur complet est dans Elasticsearch
                // Ici on stocke juste l'info sur le modèle utilisé
                embedding_model: { bsonType: 'string' },
                embedding_computed_at: { bsonType: 'date' },
                embedding_dims: { bsonType: 'int' },
                // Traçabilité
                spark_job_id: { bsonType: 'string' },
                processed_at: { bsonType: 'date' }
            }
        }
    },
    validationAction: 'warn'
});

db.articles_enriched.createIndex({ id: 1 }, { unique: true });
db.articles_enriched.createIndex({ source: 1 });
db.articles_enriched.createIndex({ publication_date: -1 });
db.articles_enriched.createIndex({ domain: 1 });
db.articles_enriched.createIndex({ embedding_model: 1 });
db.articles_enriched.createIndex({ keywords: 1 });
db.articles_enriched.createIndex(
    { title_clean: 'text', abstract_clean: 'text', keywords_extracted: 'text' },
    { name: 'enriched_text_index', weights: { title_clean: 3, keywords_extracted: 2, abstract_clean: 1 } }
);

// ============================================================
// COLLECTION : user_profiles
// Profils vectoriels des utilisateurs calculés par Spark
// Mis à jour à chaque nouvelle interaction
// ============================================================
db.createCollection('user_profiles', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['user_id'],
            properties: {
                user_id: {
                    bsonType: 'string',
                    description: 'UUID de l utilisateur (correspond à PostgreSQL)'
                },
                // Vecteur profil = moyenne pondérée des embeddings lus
                // Tableau de floats de dimension 768
                profile_vector: {
                    bsonType: 'array',
                    description: 'Embedding du profil utilisateur (768 dims)'
                },
                embedding_model: {
                    bsonType: 'string',
                    description: 'Modèle utilisé pour calculer ce profil'
                },
                // Statistiques du profil
                articles_count: {
                    bsonType: 'int',
                    description: 'Nombre d articles lus pour construire le profil'
                },
                // Top domaines et mots-clés du profil
                top_domains: { bsonType: 'array' },
                top_keywords: { bsonType: 'array' },
                // IDs des derniers articles lus (pour éviter re-recommandations)
                recent_article_ids: { bsonType: 'array' },
                computed_at: {
                    bsonType: 'date',
                    description: 'Dernière mise à jour du profil par Spark'
                }
            }
        }
    },
    validationAction: 'warn'
});

db.user_profiles.createIndex({ user_id: 1 }, { unique: true });
db.user_profiles.createIndex({ computed_at: -1 });
db.user_profiles.createIndex({ embedding_model: 1 });

// ============================================================
// COLLECTION : pipeline_logs
// Logs du pipeline Spark pour monitoring et débogage
// ============================================================
db.createCollection('pipeline_logs');

db.pipeline_logs.createIndex({ timestamp: -1 });
db.pipeline_logs.createIndex({ stage: 1 });         // bronze | silver | gold
db.pipeline_logs.createIndex({ status: 1 });         // success | error | running
db.pipeline_logs.createIndex({ spark_job_id: 1 });
// TTL index : supprime automatiquement les logs après 30 jours
db.pipeline_logs.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: 2592000, name: 'ttl_logs_30_days' }
);

// ============================================================
// DONNÉES INITIALES DE TEST
// ============================================================

// Insérer quelques articles de test pour vérifier le système
db.articles_raw.insertMany([
    {
        id: 'pubmed_test_001',
        source: 'pubmed',
        title: 'BERT-based Deep Learning for Genomic Variant Detection',
        abstract: 'We present a novel approach using BERT architecture adapted for genomic sequences. Our method achieves state-of-the-art performance on variant calling tasks across multiple datasets.',
        authors: ['Zhang, Y.', 'Li, M.', 'Wang, X.'],
        keywords: ['BERT', 'genomics', 'variant calling', 'deep learning'],
        journal: 'Bioinformatics',
        citations_count: 47,
        publication_date: '2024-01-15',
        pmid: 'test_001',
        mesh_terms: ['Deep Learning', 'Genomics', 'Sequence Analysis'],
        collected_at: new Date(),
        processing_status: 'raw'
    },
    {
        id: 'arxiv_test_002',
        source: 'arxiv',
        title: 'Transformer Models for Protein Structure Prediction: A Survey',
        abstract: 'This survey reviews the application of transformer-based models in protein structure prediction. We analyze AlphaFold2, ESMFold, and related approaches, discussing their strengths and limitations.',
        authors: ['Martin, A.', 'Dupont, B.'],
        keywords: ['transformer', 'protein structure', 'AlphaFold', 'deep learning'],
        journal: 'arXiv preprint',
        citations_count: 23,
        publication_date: '2024-02-20',
        arxiv_id: '2402.12345',
        arxiv_categories: ['cs.LG', 'q-bio.BM'],
        collected_at: new Date(),
        processing_status: 'raw'
    },
    {
        id: 'pubmed_test_003',
        source: 'pubmed',
        title: 'SciBERT for Scientific Information Extraction in Bioinformatics',
        abstract: 'SciBERT pre-trained on scientific text shows superior performance on biomedical NER and relation extraction tasks compared to general BERT models.',
        authors: ['Chen, L.', 'Kumar, R.'],
        keywords: ['SciBERT', 'NER', 'information extraction', 'bioinformatics'],
        journal: 'PLOS Computational Biology',
        citations_count: 89,
        publication_date: '2023-11-10',
        pmid: 'test_003',
        mesh_terms: ['Natural Language Processing', 'Bioinformatics'],
        collected_at: new Date(),
        processing_status: 'raw'
    }
]);

print('✅ MongoDB initialisé avec succès');
print('   Collections créées : articles_raw, articles_enriched, user_profiles, pipeline_logs');
print('   3 articles de test insérés dans articles_raw');