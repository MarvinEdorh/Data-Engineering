WITH ga_sessions AS ( 
    SELECT 
        DISTINCT (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") AS ga_session_id,
        event_timestamp,
        event_date,
        geo.country,
        device.operating_system,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "eventcategory") AS event_category,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "eventaction") AS event_action, 
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "eventlabel") AS event_label,
    FROM 
        `api-project-432166837557.analytics_150762328.events_*`
    WHERE 
        event_name = "uaevent"
        AND _TABLE_SUFFIX BETWEEN '20211210' AND '20211213'
),

facebook_event AS ( 
    SELECT 
        DISTINCT ga_sessions.ga_session_id, 
        ga_sessions.event_date,
        ga_sessions.country, 
        ga_sessions.operating_system, 
    FROM 
        ga_sessions
    WHERE 
       event_label = "facebook" OR event_action = "facebook" 
),

facebook_attempt_group1 AS ( 
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS Attempt,
    FROM 
        ga_sessions
    WHERE 
        event_action = "click"
        AND event_label = "facebook" 
    GROUP BY 1, 2, 3, 4
),

facebook_attempt_group2 AS (  
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS Attempt,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "facebook")
    GROUP BY 1, 2, 3, 4
),

facebook_attempt AS ( 
    SELECT 
        *
    FROM 
        facebook_attempt_group1
    UNION ALL
    
    SELECT 
        *    
    FROM 
        facebook_attempt_group2
),

facebook_success AS ( 
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS success, 
    FROM 
        ga_sessions
    WHERE 
        (event_action = "login-success" OR event_action = "signup-success")
        AND event_label = "facebook" 
    GROUP BY 1, 2, 3, 4
),

facebook_error AS ( 
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS error, 
    FROM 
        ga_sessions
    WHERE 
        (event_action = "error" OR event_action LIKE "%failed%")
        AND event_label = "facebook" 
    GROUP BY 1, 2, 3, 4
),

facebook AS ( 
    SELECT
        facebook_event.ga_session_id,
        facebook_event.event_date,
        facebook_event.country,
        facebook_event.operating_system,
        facebook_attempt.Attempt, 
        facebook_success.success, 
        facebook_error.error 
    FROM 
        facebook_event
    LEFT JOIN
        facebook_attempt
        ON facebook_event.ga_session_id = facebook_attempt.ga_session_id
        AND facebook_event.event_date = facebook_attempt.event_date
        AND facebook_event.country = facebook_attempt.country
        AND facebook_event.operating_system = facebook_attempt.operating_system
    LEFT JOIN
        facebook_success
        ON facebook_event.ga_session_id = facebook_success.ga_session_id
        AND facebook_event.event_date = facebook_success.event_date
        AND facebook_event.country = facebook_success.country
        AND facebook_event.operating_system = facebook_success.operating_system
    LEFT JOIN
        facebook_error
        ON facebook_event.ga_session_id = facebook_error.ga_session_id
        AND facebook_event.event_date = facebook_error.event_date
        AND facebook_event.country = facebook_error.country
        AND facebook_event.operating_system = facebook_error.operating_system
),

google_event AS (
    SELECT 
        DISTINCT ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
    FROM 
        ga_sessions
    WHERE 
       event_label = "google" OR event_action = "google" 
),

google_attempt_group1 AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS attempt,
    FROM 
        ga_sessions
    WHERE 
        event_action = "click"
        AND event_label = "google"
    GROUP BY 1, 2, 3, 4
),

google_attempt_group2 AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS attempt,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "google")
    GROUP BY 1, 2, 3, 4
),

google_attempt AS ( 
    SELECT 
       *
    FROM 
        google_attempt_group1
    
    UNION ALL
    
    SELECT 
        *
    FROM 
        google_attempt_group2
),

google_success AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS success,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "login-success" OR event_action = "signup-success") 
        AND event_label = "google"
    GROUP BY 1, 2, 3, 4
),

google_error AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS error,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "error" OR event_action LIKE "%failed%")
        AND event_label = "google"
    GROUP BY 1, 2, 3, 4
),

google AS (
    SELECT
        google_event.ga_session_id,
        google_event.event_date,
        google_event.country,
        google_event.operating_system,
        google_attempt.Attempt,
        google_success.success,
        google_error.error
    FROM 
        google_event
    LEFT JOIN
        google_attempt
        ON google_event.ga_session_id = google_attempt.ga_session_id
        AND google_event.event_date = google_attempt.event_date
        AND google_event.country = google_attempt.country
        AND google_event.operating_system = google_attempt.operating_system
    LEFT JOIN
        google_success
        ON google_event.ga_session_id = google_success.ga_session_id
        AND google_event.event_date = google_success.event_date
        AND google_event.country = google_success.country
        AND google_event.operating_system = google_success.operating_system
    LEFT JOIN
        google_error
        ON google_event.ga_session_id = google_error.ga_session_id
        AND google_event.event_date = google_error.event_date
        AND google_event.country = google_error.country
        AND google_event.operating_system = google_error.operating_system
),

email_event AS (
    SELECT 
        DISTINCT ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
    FROM 
        ga_sessions
    WHERE 
        event_label = "form" OR event_action = "form" OR event_action = "submit-email" 
),

email_attempt AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS Attempt,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "form" OR event_action = "submit-email")
        AND (event_category = "signup-form" OR event_category = "login-form" OR event_category = "form") 
    GROUP BY 1, 2, 3, 4
),

email_success AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS success,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "login-success" OR event_action = "signup-success") 
        AND event_label = "form"
    GROUP BY 1, 2, 3, 4
),

email_error AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS error,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "error" OR event_action = "signup-failed" OR event_action = "login-failed")
        AND event_label = "form"
    GROUP BY 1, 2, 3, 4
),

email AS (
    SELECT
        email_event.ga_session_id,
        email_event.event_date,
        email_event.country,
        email_event.operating_system,
        email_attempt.Attempt,
        email_success.success,
        email_error.error
    FROM 
        email_event
    LEFT JOIN
        email_attempt
        ON email_event.ga_session_id = email_attempt.ga_session_id
        AND email_event.event_date = email_attempt.event_date
        AND email_event.country = email_attempt.country
        AND email_event.operating_system = email_attempt.operating_system
    LEFT JOIN
        email_success
        ON email_event.ga_session_id = email_success.ga_session_id
        AND email_event.event_date = email_success.event_date
        AND email_event.country = email_success.country
        AND email_event.operating_system = email_success.operating_system
    LEFT JOIN
        email_error
        ON email_event.ga_session_id = email_error.ga_session_id
        AND email_event.event_date = email_error.event_date
        AND email_event.country = email_error.country
        AND email_event.operating_system = email_error.operating_system
),

msisdn_event AS (
    SELECT 
        DISTINCT ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
    FROM 
        ga_sessions
    WHERE 
        event_label = "msisdn" OR event_action = "msisdn" OR event_action = "submit-phone" 
),

msisdn_attempt AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS Attempt,
    FROM 
        ga_sessions
    WHERE 
        (event_category = "signup-form" OR event_category = "login-form" OR event_category = "form") 
        AND (event_action = "msisdn" OR event_action = "submit-phone")
    GROUP BY 1, 2, 3, 4
),

msisdn_success AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS success,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "login-success" OR event_action = "signup-success") 
        AND event_label = "msisdn"
    GROUP BY 1, 2, 3, 4
),

msisdn_error AS (
    SELECT 
        ga_sessions.ga_session_id,
        ga_sessions.event_date,
        ga_sessions.country,
        ga_sessions.operating_system,
        COUNT(DISTINCT ga_sessions.event_timestamp) AS error,
    FROM 
        ga_sessions
    WHERE 
        (event_action = "error" OR event_action = "signup-failed" OR event_action = "login-failed")
        AND (event_category LIKE "%form%" OR event_category = "signup" OR event_category = "login") 
        AND event_label = "msisdn"
    GROUP BY 1, 2, 3, 4
),

msisdn AS (
    SELECT
        msisdn_event.ga_session_id,
        msisdn_event.event_date,
        msisdn_event.country,
        msisdn_event.operating_system,
        msisdn_attempt.Attempt,
        msisdn_success.success,
        msisdn_error.error,
    FROM 
        msisdn_event
    LEFT JOIN
        msisdn_attempt
        ON msisdn_event.ga_session_id = msisdn_attempt.ga_session_id
        AND msisdn_event.event_date = msisdn_attempt.event_date
        AND msisdn_event.country = msisdn_attempt.country
        AND msisdn_event.operating_system = msisdn_attempt.operating_system
    LEFT JOIN
        msisdn_success
        ON msisdn_event.ga_session_id = msisdn_success.ga_session_id
        AND msisdn_event.event_date = msisdn_success.event_date
        AND msisdn_event.country = msisdn_success.country
        AND msisdn_event.operating_system = msisdn_success.operating_system
    LEFT JOIN
        msisdn_error
        ON msisdn_event.ga_session_id = msisdn_error.ga_session_id
        AND msisdn_event.event_date = msisdn_error.event_date
        AND msisdn_event.country = msisdn_error.country
        AND msisdn_event.operating_system = msisdn_error.operating_system
),

method AS (
    SELECT
        *,
        "facebook" AS method
    FROM
        facebook
    WHERE Attempt IS NOT NULL

    UNION ALL

    SELECT 
        *,
        "google" AS method
    FROM
        google
    WHERE Attempt IS NOT NULL

    UNION ALL

    SELECT 
        *,
        "email" AS method
    FROM
        email
    WHERE Attempt IS NOT NULL

    UNION ALL

    SELECT 
        *,
        "msisdn" AS method
    FROM
        msisdn
    WHERE Attempt IS NOT NULL
)

SELECT
    event_date,
    country,
    operating_system,
    method.method,
    SUM(Attempt) total_attempt,
    SUM(success) total_success,
    SUM(error) total_error,
    COUNT(DISTINCT ga_session_id) AS unique_attempt,
    SUM(CASE WHEN success IS NOT NULL THEN 1 ELSE NULL END) AS unique_success, 
    SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE NULL END) AS unique_error, 
FROM 
    method
GROUP BY 1, 2, 3, 4
