USE telecom_db;

-- Create Tele Traffic Call Details View
CREATE VIEW `call_details_view` AS
SELECT
    c.ID AS CALL_ID,
    callerUser.USER_NAME AS CALLER_USER_NAME,
    calleeUser.USER_NAME AS CALLEE_USER_NAME,
    callerTower.TOWER_NAME AS CALLER_TOWER_NAME,
    calleeTower.TOWER_NAME AS CALLEE_TOWER_NAME,
    DATE_FORMAT(c.CALL_START_DATE_TIME, '%Y-%m-%d') AS CALL_START_DATE,
    DATE_FORMAT(c.CALL_START_DATE_TIME, '%H:%i:%s') AS CALL_START_TIME,
    DATE_FORMAT(c.CALL_END_DATE_TIME, '%Y-%m-%d') AS CALL_END_DATE,
    DATE_FORMAT(c.CALL_END_DATE_TIME, '%H:%i:%s') AS CALL_END_TIME,
    TIMEDIFF(c.CALL_END_DATE_TIME, c.CALL_START_DATE_TIME) AS CALL_DURATION,
    c.CALL_TYPE AS CALL_TYPE
FROM
    calls c
    JOIN users callerUser ON c.CALLER_USER_ID = callerUser.ID
    JOIN users calleeUser ON c.CALEE_USER_ID = calleeUser.ID
    JOIN cell_towers callerTower ON c.CALLER_TOWER_ID = callerTower.ID
    JOIN cell_towers calleeTower ON c.CALEE_TOWER_ID = calleeTower.ID
ORDER BY
    CALL_ID;