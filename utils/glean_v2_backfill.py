# For https://mozilla-hub.atlassian.net/browse/DENG-8494
# Should match https://github.com/mozilla/mozilla-schema-generator/blob/main/mozilla_schema_generator/configs/glean_v2_allowlist.yaml
column_removal_backfill_tables = [
    "org_mozilla_fenix_nightly_stable.metrics_v1",
    "org_mozilla_fenix_nightly_stable.sync_v1",
    "org_mozilla_fenix_nightly_stable.first_session_v1",
    "org_mozilla_fenix_nightly_stable.adjust_attribution_v1",
    "org_mozilla_fenix_nightly_stable.health_v1",
    "org_mozilla_fenix_nightly_stable.captcha_detection_v1",
    "org_mozilla_fennec_aurora_stable.metrics_v1",
    "org_mozilla_fennec_aurora_stable.sync_v1",
    "org_mozilla_fennec_aurora_stable.captcha_detection_v1",
    "org_mozilla_fennec_aurora_stable.first_session_v1",
    "org_mozilla_fennec_aurora_stable.health_v1",
    "org_mozilla_fennec_aurora_stable.adjust_attribution_v1",
]
