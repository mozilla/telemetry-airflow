from prio.processor import (
    prio_staging_bootstrap,
    prio_staging,
    copy_staging_data_to_server_a,
    copy_staging_data_to_server_b,
    prio_a,
    prio_b,
)

prio_staging_bootstrap >> prio_staging
prio_staging >> copy_staging_data_to_server_a >> prio_a
prio_staging >> copy_staging_data_to_server_b >> prio_b
