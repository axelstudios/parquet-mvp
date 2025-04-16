import {createApp} from 'vue/dist/vue.esm-browser.prod.js'
import {asyncBufferFromUrl, parquetReadObjects} from 'hyparquet'
import {compressors} from 'hyparquet-compressors'
import {columns, upgrades} from './columns'
import {convertToCsv, downloadCsv} from './downloadCsv'
import {countyLookup, stateLookup} from './geo'

createApp({
  data() {
    return {
      // Filters
      stateLookup,
      selectedStates: [],
      countyLookup,
      selectedCounties: [],
      columns,
      selectedColumns: columns.slice(0, 5),
      upgrades,
      selectedUpgrades: [],
      // Outputs
      urlGroups: [],
      isDownloading: false,
      completed: 0,
    }
  },
  computed: {
    sortedStates() {
      return Object.entries(stateLookup).sort(([, stateA], [, stateB]) => stateA.localeCompare(stateB))
    },
    nestedCounties() {
      const counties = {}
      for (const county of this.selectedCounties) {
        for (const stateFips of this.selectedStates) {
          if (county in countyLookup[stateFips]) {
            counties[stateFips] ??= []
            counties[stateFips].push(county)
            break
          }
        }
      }
      return counties
    },
    totalUrls() {
      return this.urlGroups.reduce((total, group) => total + group.length, 0)
    }
  },
  methods: {
    async download() {
      this.isDownloading = true
      this.completed = 0

      const urlGroups = []
      for (const stateFips of this.selectedStates) {
        const state = stateLookup[stateFips]
        for (const county of this.nestedCounties[stateFips]) {
          const urlsForUpgrade = []
          for (const upgrade of ['baseline', ...this.selectedUpgrades]) {
            urlsForUpgrade.push(`https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results_aggregates/by_state_and_county/full/parquet/state%3D${state}/county%3D${county}/${state}_${county}_${upgrade}_agg.parquet`)
          }
          urlGroups.push(urlsForUpgrade)
        }
      }
      this.urlGroups = urlGroups

      const data = []
      for (const urls of urlGroups) {
        for (const url of urls) {
          let file
          try {
            file = await asyncBufferFromUrl({url})
          } catch (err) {
            ++this.completed
            continue
          }

          data.push(...await parquetReadObjects({file, compressors, columns: this.selectedColumns}))
          ++this.completed
        }
      }
      if (data.length > 0) {
        downloadCsv(convertToCsv(data, this.selectedColumns), 'Merged Data.csv')
      }

      this.isDownloading = false
    },
  },
}).mount('#app')
