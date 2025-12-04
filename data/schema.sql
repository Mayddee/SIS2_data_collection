-- SQLite Database Schema for sxodim.com events
-- This table stores cleaned event data from the website

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,              -- Unique event identifier
    title TEXT NOT NULL,              -- Event title
    url TEXT NOT NULL,                -- Event page URL
    category TEXT,                    -- Event category (concert, theater, etc.)
    min_price TEXT,                   -- Minimum ticket price
    partner TEXT,                     -- Event partner/organizer
    views INTEGER,                    -- Number of views
    date TEXT,                        -- Event date
    time TEXT,                        -- Event time
    address TEXT,                     -- Event venue address
    description TEXT,                 -- Full event description
    publication_date TEXT,            -- When event was published
    tags TEXT                         -- Comma-separated tags
);
