-- 1. Static User Profile Table
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    age INT NOT NULL,
    country VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Dynamic Interaction Event Stream Table
CREATE TABLE IF NOT EXISTS user_interactions (
    event_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    item_id INT NOT NULL,
    category VARCHAR(50) NOT NULL,
    interaction_type VARCHAR(20) NOT NULL, -- e.g., 'click', 'view', 'purchase'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Composite Index for Analytical Point-in-Time Queries
CREATE INDEX IF NOT EXISTS idx_user_time 
ON user_interactions (user_id, created_at DESC);
