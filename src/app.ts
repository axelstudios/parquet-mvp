import { parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { getS3File } from './getS3File.ts'

// Constants
const key_base = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results_aggregates/by_state_and_county/full/parquet/'

// Config
const state = 'CO'
const county = 'G0800590'
const upgrade = 'baseline'
const columns = ['bldg_id', 'upgrade']

const file = await getS3File(`${key_base}state=${state}/county=${county}/${state}_${county}_${upgrade}_agg.parquet`)

if (file) {
  const data = await parquetReadObjects({file, compressors, columns})

  console.log(data)
}
