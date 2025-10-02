<img width="1544" height="444" alt="image" src="https://github.com/user-attachments/assets/a73b7aa4-a61a-40c6-ae55-4c3b0b8f14bb" />

## 🐊 Data Flow
Execute run_all.sh to go through the appropriate run steps.

1. **RAW (seeds/)**  
   - CSVs are loaded via python into Neon RAW schema.  
   - Example: `RAW.crocodile_species`.
  
3. **CHECKS (models/checks/)**
   - null checks
   - duplicates (including nulls)
   - duplicates (excluding nulls)
   - Accepted values checks
   - Numeric checks
   - Composite uniqueness checks
   - Date parameter checks
   - Crossfield Logic Checks
   - freshness check
   - unparsable_dates check

2. **STAGING (models/staging/)**  
   - Standardizes column names and types.  
   - Handles light cleaning/checks.
   - Still at row-level grain.

3. **GOLDEN (models/golden/)**  
   - **Dimensions**: `dim_species`, `dim_location`, `dim_date`, `dim_observer`, `dim_conservation_status`.  
   - **Fact**: `fact_croc_observation` with measures (`observed_length_m`, `observed_weight_kg`) and foreign keys to all dims.  

4. **Folders** 
  - checks/ - qa checks (joined to staging to remove invalid observation ids).
	- scripts/ - helper ingestion scripts (fetch CSVs).
	- staging/ - row-level cleanup joined to checks/.
	- golden/ - star schema (dims and fact) for BI & semantic layers.
  - raw/ - create table if not existent
  - csv/ - stores the csv pulled from py script
