-- qos--1.0.sql
-- PostgreSQL QoS Resource Governor Extension
-- SQL install script for the QoS extension

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION qos" to load this file. \quit

-- Function: qos_version()
-- Returns the version of the QoS extension
CREATE FUNCTION qos_version() 
RETURNS text
LANGUAGE C IMMUTABLE STRICT
AS '$libdir/qos', 'qos_version';

-- Function: qos_get_stats()
-- Returns current QoS statistics
CREATE FUNCTION qos_get_stats()
RETURNS text
LANGUAGE C STRICT
AS '$libdir/qos', 'qos_get_stats';

-- Function: qos_reset_stats()
-- Resets QoS statistics
CREATE FUNCTION qos_reset_stats()
RETURNS void
LANGUAGE C STRICT
AS '$libdir/qos', 'qos_reset_stats';

-- View: qos_rsettings
-- Shows current QoS settings for all roles and databases using pg_db_role_setting
CREATE VIEW qos_settings AS
SELECT 
    COALESCE(r.rolname, 'database-wide') as rolname,
    COALESCE(d.datname, 'cluster-wide') as datname,
    cfg as setting
FROM pg_db_role_setting s
LEFT JOIN pg_roles r ON r.oid = s.setrole
LEFT JOIN pg_database d ON d.oid = s.setdatabase,
LATERAL unnest(s.setconfig) cfg
WHERE cfg LIKE 'qos.%';

-- Extension metadata
COMMENT ON EXTENSION qos IS 'PostgreSQL Quality of Service (QoS) Resource Governor';
COMMENT ON FUNCTION qos_version() IS 'Returns QoS extension version';
COMMENT ON FUNCTION qos_get_stats() IS 'Returns current QoS statistics';
COMMENT ON FUNCTION qos_reset_stats() IS 'Resets QoS statistics counters';