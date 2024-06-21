USE telecom_db;

-- Create Tele Traffic Call Details View
CREATE VIEW `call_details_view` AS
SELECT
    calls.ID AS CALL_ID,
    callerUser.USER_NAME AS CALLER_USER_NAME,
    calleeUser.USER_NAME AS CALLEE_USER_NAME,
    callerTower.TOWER_NAME AS CALLER_TOWER_NAME,
    calleeTower.TOWER_NAME AS CALLEE_TOWER_NAME,
    calls.CALL_START_DATE_TIME AS CALL_START_DATE_TIME,
    DATE_FORMAT(calls.CALL_START_DATE_TIME, '%Y-%m-%d') AS CALL_START_DATE,
    DATE_FORMAT(calls.CALL_START_DATE_TIME, '%H:%i:%s') AS CALL_START_TIME,
    calls.CALL_END_DATE_TIME AS CALL_END_DATE_TIME,
    DATE_FORMAT(calls.CALL_END_DATE_TIME, '%Y-%m-%d') AS CALL_END_DATE,
    DATE_FORMAT(calls.CALL_END_DATE_TIME, '%H:%i:%s') AS CALL_END_TIME,
    calls.CALL_TYPE AS CALL_TYPE
FROM
    calls calls
    JOIN users callerUser ON calls.CALLER_USER_ID = callerUser.ID
    JOIN users calleeUser ON calls.CALEE_USER_ID = calleeUser.ID
    JOIN cell_towers callerTower ON calls.CALLER_TOWER_ID = callerTower.ID
    JOIN cell_towers calleeTower ON calls.CALEE_TOWER_ID = calleeTower.ID
ORDER BY
    CALL_ID;