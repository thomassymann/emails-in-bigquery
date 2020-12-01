DECLARE startdate STRING DEFAULT "20201101";
DECLARE enddate STRING DEFAULT "20201101";

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "userId" as key, userId as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(userId, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.adContent" as key, trafficSource.adContent as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.adContent, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.campaign" as key, trafficSource.campaign as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.campaign, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.campaignCode" as key, trafficSource.campaignCode as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.campaignCode, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.keyword" as key, trafficSource.keyword as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.keyword, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.medium" as key, trafficSource.medium as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.medium, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.referralPath" as key, trafficSource.referralPath as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.referralPath, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "trafficSource.source" as key, trafficSource.source as value
FROM `<project>.<dataset>.ga_sessions_*` as t1
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(trafficSource.source, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.transaction.transactionCoupon" as key, hits.transaction.transactionCoupon as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.transaction.transactionCoupon, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.referer" as key, hits.referer as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.referer, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.page.pagePath" as key, hits.page.pagePath as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.page.pagePath, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.page.pageTitle" as key, hits.page.pageTitle as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.page.pageTitle, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.page.searchKeyword" as key, hits.page.searchKeyword as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.page.searchKeyword, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.eventInfo.eventCategory" as key, hits.eventInfo.eventCategory as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.eventInfo.eventCategory, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.eventInfo.eventAction" as key, hits.eventInfo.eventAction as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.eventInfo.eventAction, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, "hits.eventInfo.eventLabel" as key, hits.eventInfo.eventLabel as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(hits.eventInfo.eventLabel, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4

UNION ALL

SELECT
fullVisitorId, extract(date from timestamp_seconds(t1.visitStartTime)) AS date, CAST(cdh.index as string) as key, cdh.value as value
FROM `<project>.<dataset>.ga_sessions_*` as t1, UNNEST(hits) as hits, UNNEST(hits.customDimensions) as cdh
WHERE _TABLE_SUFFIX BETWEEN startdate AND enddate
AND REGEXP_CONTAINS(cdh.value, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
GROUP BY 1,2,3,4
