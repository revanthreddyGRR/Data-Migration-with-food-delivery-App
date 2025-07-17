use role sysadmin;
use schema sandbox.stage_rawdata;

create or replace table stage_rawdata.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

create or replace stream stage_rawdata.location_stm 
on table stage_rawdata.location
append_only = true
comment = 'this is the append-only stream object on location table that gets delta data based on changes';

select * from stage_rawdata.location;

copy into stage_rawdata.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_rawdata.csv_stg/initial/location t
)
file_format = (format_name = 'stage_rawdata.csv_file_format')
on_error = abort_statement;

select *
from table(information_schema.copy_history(table_name=>'LOCATION', start_time=> dateadd(hours, -1, current_timestamp())));


select * from stage_rawdata.location;
select * from stage_rawdata.location_stm;

use schema clean_datacleaning;

-- Level 2
create or replace table clean_datacleaning.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    --is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream clean_datacleaning.restaurant_location_stm 
on table clean_datacleaning.restaurant_location
comment = 'this is a standard stream object on the location table to track insert, update, and delete changes';


MERGE INTO clean_datacleaning.restaurant_location AS target
USING (
    SELECT 
        CAST(LocationID AS NUMBER) AS Location_ID,
        CAST(City AS STRING) AS City,
        CASE 
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS State,
        -- State Code Mapping
        CASE 
            WHEN State = 'California' THEN 'CA'
WHEN State = 'Texas' THEN 'TX'
WHEN State = 'New York' THEN 'NY'
WHEN State = 'Florida' THEN 'FL'
WHEN State = 'Illinois' THEN 'IL'
WHEN State = 'Pennsylvania' THEN 'PA'
WHEN State = 'Ohio' THEN 'OH'
WHEN State = 'Georgia' THEN 'GA'
WHEN State = 'North Carolina' THEN 'NC'
WHEN State = 'Michigan' THEN 'MI'
WHEN State = 'New Jersey' THEN 'NJ'
WHEN State = 'Virginia' THEN 'VA'
WHEN State = 'Washington' THEN 'WA'
WHEN State = 'Arizona' THEN 'AZ'
WHEN State = 'Massachusetts' THEN 'MA'
WHEN State = 'Tennessee' THEN 'TN'
WHEN State = 'Indiana' THEN 'IN'
WHEN State = 'Missouri' THEN 'MO'
WHEN State = 'Wisconsin' THEN 'WI'
WHEN State = 'Colorado' THEN 'CO'
WHEN State = 'Minnesota' THEN 'MN'
WHEN State = 'South Carolina' THEN 'SC'
WHEN State = 'Alabama' THEN 'AL'
WHEN State = 'Louisiana' THEN 'LA'
WHEN State = 'Kentucky' THEN 'KY'
WHEN State = 'Oregon' THEN 'OR'
WHEN State = 'Oklahoma' THEN 'OK'
WHEN State = 'Connecticut' THEN 'CT'
WHEN State = 'Iowa' THEN 'IA'
WHEN State = 'Nevada' THEN 'NV'
WHEN State = 'Arkansas' THEN 'AR'
WHEN State = 'Mississippi' THEN 'MS'
WHEN State = 'Utah' THEN 'UT'
WHEN State = 'Kansas' THEN 'KS'
WHEN State = 'New Mexico' THEN 'NM'
WHEN State = 'Nebraska' THEN 'NE'
WHEN State = 'West Virginia' THEN 'WV'
WHEN State = 'Idaho' THEN 'ID'
WHEN State = 'Hawaii' THEN 'HI'
WHEN State = 'New Hampshire' THEN 'NH'
WHEN State = 'Maine' THEN 'ME'
WHEN State = 'Rhode Island' THEN 'RI'
WHEN State = 'Montana' THEN 'MT'
WHEN State = 'Delaware' THEN 'DE'
WHEN State = 'South Dakota' THEN 'SD'
WHEN State = 'North Dakota' THEN 'ND'
WHEN State = 'Alaska' THEN 'AK'
WHEN State = 'Vermont' THEN 'VT'
WHEN State = 'Wyoming' THEN 'WY'
WHEN State = 'District of Columbia' THEN 'DC'
    ELSE NULL
        END AS state_code,
   CASE
    WHEN (State = 'Alabama' AND City = 'Birmingham') THEN TRUE
    WHEN (State = 'Alaska' AND City = 'Anchorage') THEN TRUE
    WHEN (State = 'Arizona' AND City = 'Phoenix') THEN TRUE
    WHEN (State = 'Arkansas' AND City = 'Little Rock') THEN TRUE
    WHEN (State = 'California' AND City = 'Los Angeles') THEN TRUE
    WHEN (State = 'Colorado' AND City = 'Denver') THEN TRUE
    WHEN (State = 'Connecticut' AND City = 'Bridgeport') THEN TRUE
    WHEN (State = 'Delaware' AND City = 'Wilmington') THEN TRUE
    WHEN (State = 'Florida' AND City = 'Miami') THEN TRUE
    WHEN (State = 'Georgia' AND City = 'Atlanta') THEN TRUE
    WHEN (State = 'Hawaii' AND City = 'Honolulu') THEN TRUE
    WHEN (State = 'Idaho' AND City = 'Boise') THEN TRUE
    WHEN (State = 'Illinois' AND City = 'Chicago') THEN TRUE
    WHEN (State = 'Indiana' AND City = 'Indianapolis') THEN TRUE
    WHEN (State = 'Iowa' AND City = 'Des Moines') THEN TRUE
    WHEN (State = 'Kansas' AND City = 'Wichita') THEN TRUE
    WHEN (State = 'Kentucky' AND City = 'Louisville') THEN TRUE
    WHEN (State = 'Louisiana' AND City = 'New Orleans') THEN TRUE
    WHEN (State = 'Maine' AND City = 'Portland') THEN TRUE
    WHEN (State = 'Maryland' AND City = 'Baltimore') THEN TRUE
    WHEN (State = 'Massachusetts' AND City = 'Boston') THEN TRUE
    WHEN (State = 'Michigan' AND City = 'Detroit') THEN TRUE
    WHEN (State = 'Minnesota' AND City = 'Minneapolis') THEN TRUE
    WHEN (State = 'Mississippi' AND City = 'Jackson') THEN TRUE
    WHEN (State = 'Missouri' AND City = 'Kansas City') THEN TRUE
    WHEN (State = 'Montana' AND City = 'Billings') THEN TRUE
    WHEN (State = 'Nebraska' AND City = 'Omaha') THEN TRUE
    WHEN (State = 'Nevada' AND City = 'Las Vegas') THEN TRUE
    WHEN (State = 'New Hampshire' AND City = 'Manchester') THEN TRUE
    WHEN (State = 'New Jersey' AND City = 'Newark') THEN TRUE
    WHEN (State = 'New Mexico' AND City = 'Albuquerque') THEN TRUE
    WHEN (State = 'New York' AND City = 'New York') THEN TRUE
    WHEN (State = 'North Carolina' AND City = 'Charlotte') THEN TRUE
    WHEN (State = 'North Dakota' AND City = 'Fargo') THEN TRUE
    WHEN (State = 'Ohio' AND City = 'Columbus') THEN TRUE
    WHEN (State = 'Oklahoma' AND City = 'Oklahoma City') THEN TRUE
    WHEN (State = 'Oregon' AND City = 'Portland') THEN TRUE
    WHEN (State = 'Pennsylvania' AND City = 'Philadelphia') THEN TRUE
    WHEN (State = 'Rhode Island' AND City = 'Providence') THEN TRUE
    WHEN (State = 'South Carolina' AND City = 'Charleston') THEN TRUE
    WHEN (State = 'South Dakota' AND City = 'Sioux Falls') THEN TRUE
    WHEN (State = 'Tennessee' AND City = 'Nashville') THEN TRUE
    WHEN (State = 'Texas' AND City = 'Houston') THEN TRUE
    WHEN (State = 'Utah' AND City = 'Salt Lake City') THEN TRUE
    WHEN (State = 'Vermont' AND City = 'Burlington') THEN TRUE
    WHEN (State = 'Virginia' AND City = 'Virginia Beach') THEN TRUE
    WHEN (State = 'Washington' AND City = 'Seattle') THEN TRUE
    WHEN (State = 'West Virginia' AND City = 'Charleston') THEN TRUE
    WHEN (State = 'Wisconsin' AND City = 'Milwaukee') THEN TRUE
    WHEN (State = 'Wyoming' AND City = 'Cheyenne') THEN TRUE
    -- U.S. Territories
    WHEN (State = 'District of Columbia' AND City = 'Washington') THEN TRUE
    WHEN (State = 'Puerto Rico' AND City = 'San Juan') THEN TRUE
    WHEN (State = 'Guam' AND City = 'Hagåtña') THEN TRUE
    WHEN (State = 'U.S. Virgin Islands' AND City = 'Charlotte Amalie') THEN TRUE
    WHEN (State = 'Northern Mariana Islands' AND City = 'Saipan') THEN TRUE
    WHEN (State = 'American Samoa' AND City = 'Pago Pago') THEN TRUE
    ELSE FALSE
END AS capital_city_flag,
       CASE 
    WHEN City IN ('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego') THEN 'Tier-1'
    WHEN City IN ('Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte', 'Indianapolis', 
                  'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville') THEN 'Tier-2'
    ELSE 'Tier-3'
END AS city_tier,
CAST(ZipCode AS STRING) AS Zip_Code,
CAST(ActiveFlag AS STRING) AS Active_Flag,
TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
CURRENT_TIMESTAMP AS _copy_data_ts
FROM stage_rawdata.location_stm
) AS source
ON target.Location_ID = source.Location_ID
WHEN MATCHED AND (
    target.City != source.City OR
    target.State != source.State OR
    target.state_code != source.state_code OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.Zip_Code != source.Zip_Code OR
    target.Active_Flag != source.Active_Flag OR
    target.modified_ts != source.modified_ts
) THEN 
    UPDATE SET 
        target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.Zip_Code = source.Zip_Code,
        target.Active_Flag = source.Active_Flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        Location_ID,
        City,
        State,
        state_code,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

create or replace table consumption.restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,                      -- hash key for the dimension
    location_id number(38,0) not null,                  -- business key
    city varchar(100) not null,                         -- city
    state varchar(100) not null,                        -- state
    state_code varchar(2) not null,                     -- state code
   --- is_union_territory boolean not null default false,   -- union territory flag
    capital_city_flag boolean not null default false,     -- capital city flag
    city_tier varchar(6),                               -- city tier
    zip_code varchar(10) not null,                      -- zip code
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true         -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';


MERGE INTO 
        CONSUMPTION.RESTAURANT_LOCATION_DIM AS target
    USING 
        CLEAN_datacleaning.RESTAURANT_LOCATION_STM AS source
    ON 
        target.LOCATION_ID = source.LOCATION_ID and 
        target.ACTIVE_FLAG = source.ACTIVE_FLAG
    WHEN MATCHED 
        AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DT = CURRENT_TIMESTAMP(),
        target.CURRENT_FLAG = FALSE
    WHEN NOT MATCHED 
        AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
    THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
    WHEN NOT MATCHED AND 
    source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

-- Part-2
copy into stage_rawdata.location (locationid, city, state, zipcode, activeflag, 
                    createddate, modifieddate, _stg_file_name, 
                    _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_rawdata.csv_stg/delta/location/delta-day02-2rows-update.csv t
)
file_format = (format_name = 'stage_rawdata.csv_file_format')
on_error = abort_statement;
