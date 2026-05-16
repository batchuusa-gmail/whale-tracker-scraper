-- Migration 001: Add position_size_pct to algo_config and create algo_settings
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/bedurjtazsfbnkisoeee/sql/new

-- 1. Add position_size_pct to existing algo_config table
ALTER TABLE algo_config ADD COLUMN IF NOT EXISTS position_size_pct FLOAT DEFAULT 0.03;

-- Update existing row with the new value
UPDATE algo_config SET position_size_pct = 0.03 WHERE id = 1;

-- 2. Create algo_settings table (primary config source going forward)
CREATE TABLE IF NOT EXISTS algo_settings (
    id                  SERIAL PRIMARY KEY,
    min_score           INTEGER DEFAULT 60,
    min_confidence      INTEGER DEFAULT 65,   -- stored as 0-100, divided by 100 in scanner
    max_trades_per_day  INTEGER DEFAULT 6,
    max_positions       INTEGER DEFAULT 3,
    position_size_pct   FLOAT   DEFAULT 0.03,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Enable RLS
ALTER TABLE algo_settings ENABLE ROW LEVEL SECURITY;

-- Allow anon read (scanner uses anon key)
CREATE POLICY "allow anon read" ON algo_settings FOR SELECT USING (true);

-- Seed with current values
INSERT INTO algo_settings (id, min_score, min_confidence, max_trades_per_day, max_positions, position_size_pct)
VALUES (1, 60, 65, 6, 3, 0.03)
ON CONFLICT (id) DO UPDATE SET
    min_score          = EXCLUDED.min_score,
    min_confidence     = EXCLUDED.min_confidence,
    max_trades_per_day = EXCLUDED.max_trades_per_day,
    max_positions      = EXCLUDED.max_positions,
    position_size_pct  = EXCLUDED.position_size_pct,
    updated_at         = NOW();
