select *
from {{ source("raw", "redcap_data") }}

