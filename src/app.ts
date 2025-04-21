import { asyncBufferFromUrl, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { columns, upgrades } from './columns'
import { convertToCsv, downloadCsv } from './downloadCsv'
import { countyLookup, stateLookup } from './geo'
import { personas } from './personas'
import { uploadS3File } from './s3'

type CountyMap = Record<string, string[]>

class ParquetMerge {
  // ——— State ———
  firstName = ''
  lastName = ''
  email = ''
  useCase = ''
  personas = personas
  selectedPersona = ''

  stateLookup = stateLookup
  selectedStates: string[] = []
  countyLookup = countyLookup
  selectedCounties: string[] = []

  columns = columns
  selectedColumns = columns.slice(0, 5)
  upgrades = upgrades
  selectedUpgrades: string[] = []

  urlGroups: string[][] = []
  isDownloading = false
  completed = 0

  constructor() {
    this.bindElements()
    this.renderPersonaOptions()
    this.renderStateOptions()
    this.renderCountyOptions()
    this.renderColumnOptions()
    this.renderUpgradeOptions()
    this.updateDownloadButton()
  }

  // ——— Computed getters ———
  get sortedStates(): [string, string][] {
    return Object.entries(this.stateLookup).sort(([, a], [, b]) => a.localeCompare(b))
  }

  get nestedCounties(): CountyMap {
    const out: CountyMap = {}
    for (const county of this.selectedCounties) {
      for (const stateFips of this.selectedStates) {
        if (this.countyLookup[stateFips] && county in this.countyLookup[stateFips]) {
          out[stateFips] = out[stateFips] || []
          out[stateFips].push(county)
          break
        }
      }
    }
    return out
  }

  get totalUrls(): number {
    return this.urlGroups.reduce((sum, grp) => sum + grp.length, 0)
  }

  get downloadDisabled(): boolean {
    return (
      this.isDownloading ||
      [this.firstName, this.lastName, this.email, this.useCase, this.selectedPersona].some((v) => !v.trim()) ||
      [this.selectedStates, this.selectedCounties, this.selectedColumns, this.selectedUpgrades].some((arr) => arr.length === 0)
    )
  }

  // ——— Setup and render methods ———
  private bindElements() {
    // Text inputs
    this.bindInput('firstName', (v) => (this.firstName = v))
    this.bindInput('lastName', (v) => (this.lastName = v))
    this.bindInput('email', (v) => (this.email = v))
    this.bindInput('useCase', (v) => (this.useCase = v))

    // Persona select
    this.bindSelect('personaSelect', (opts) => {
      this.selectedPersona = opts[0] || ''
      this.updateDownloadButton()
    })

    // State multiselect
    this.bindSelect('statesSelect', (opts) => {
      this.selectedStates = opts
      this.renderCountyOptions()
      this.updateDownloadButton()
    })

    // County multiselect
    this.bindSelect('countiesSelect', (opts) => {
      this.selectedCounties = opts
      this.updateDownloadButton()
    })

    // Columns multiselect
    this.bindSelect('columnsSelect', (opts) => {
      this.selectedColumns = opts as typeof columns
      this.updateDownloadButton()
    })

    // Upgrades multiselect
    this.bindSelect('upgradesSelect', (opts) => {
      this.selectedUpgrades = opts
      this.updateDownloadButton()
    })

    document.getElementById('downloadBtn')!.addEventListener('click', () => this.download())
  }

  private bindInput(id: string, setter: (val: string) => void) {
    const el = document.getElementById(id) as HTMLInputElement
    el.addEventListener('input', () => {
      setter(el.value)
      this.updateDownloadButton()
    })
  }

  private bindSelect(id: string, setter: (values: string[]) => void) {
    const sel = document.getElementById(id) as HTMLSelectElement
    sel.addEventListener('change', () => {
      const vals = Array.from(sel.selectedOptions).map((o) => o.value)
      setter(vals)
    })
  }

  private renderPersonaOptions() {
    const sel = document.getElementById('personaSelect') as HTMLSelectElement
    sel.innerHTML =
      '<option disabled value="" selected>Persona</option>' + this.personas.map((persona) => `<option value="${persona}">${persona}</option>`).join('')
  }

  private renderStateOptions() {
    const sel = document.getElementById('statesSelect') as HTMLSelectElement
    sel.innerHTML =
      '<option disabled value="">Please select states</option>' + this.sortedStates.map(([fips, name]) => `<option value="${fips}">${name}</option>`).join('')
  }

  private renderCountyOptions() {
    const sel = document.getElementById('countiesSelect') as HTMLSelectElement
    sel.innerHTML =
      '<option disabled value="">Please select counties</option>' +
      this.selectedStates
        .flatMap((fips) => [
          `<option disabled style="align-items: center; background-color: #999; color: white; display: flex; height: 24px; justify-content: center">${stateLookup[fips]}</option>`,
          ...Object.entries(this.countyLookup[fips] ?? {}).map(([id, name]) => `<option value="${id}">${name}</option>`),
        ])
        .join('')
  }

  private renderColumnOptions() {
    const sel = document.getElementById('columnsSelect') as HTMLSelectElement
    sel.innerHTML =
      '<option disabled value="">Please select columns</option>' +
      this.columns.map((column) => `<option value="${column}" ${this.selectedColumns.includes(column) ? 'selected' : ''}>${column}</option>`).join('')
  }

  private renderUpgradeOptions() {
    const sel = document.getElementById('upgradesSelect') as HTMLSelectElement
    sel.innerHTML =
      '<option disabled value="">Please select upgrades</option>' + this.upgrades.map((upgrade) => `<option value="${upgrade}">${upgrade}</option>`).join('')
  }

  private updateDownloadButton() {
    const btn = document.getElementById('downloadBtn') as HTMLButtonElement
    const total = this.totalUrls
    btn.textContent = this.isDownloading && total > 0 ? `Downloading ${this.completed + 1} / ${this.totalUrls}` : 'Download'
    btn.disabled = this.downloadDisabled
  }

  // ——— Actions ———
  private async download() {
    this.completed = 0
    this.isDownloading = true
    this.updateDownloadButton()

    await this.uploadUserQuery()

    // build URL groups
    const groups: string[][] = []
    for (const fips of this.selectedStates) {
      const stateName = this.stateLookup[fips]
      for (const county of this.nestedCounties[fips] || []) {
        groups.push(
          ['baseline', ...this.selectedUpgrades].map(
            (upgrade) =>
              `https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results_aggregates/by_state_and_county/full/parquet/state%3D${stateName}/county%3D${county}/${stateName}_${county}_${upgrade}_agg.parquet`,
          ),
        )
      }
    }
    this.urlGroups = groups
    this.updateDownloadButton()

    // fetch and parse
    const allData: any[] = []
    for (const grp of groups) {
      for (const url of grp) {
        try {
          const file = await asyncBufferFromUrl({ url })
          const objs = await parquetReadObjects({
            file,
            compressors,
            columns: this.selectedColumns,
          })
          allData.push(...objs)
        } catch {
          // no-op
        }
        this.completed++
        this.updateDownloadButton()
      }
    }

    if (allData.length) {
      const csv = convertToCsv(allData, this.selectedColumns)
      downloadCsv(csv, 'Merged Data.csv')
    }

    this.isDownloading = false
    this.updateDownloadButton()
  }

  private async uploadUserQuery() {
    const countyList = Object.entries(this.nestedCounties).flatMap(([fips, ids]) => ids.map((id) => `${this.countyLookup[fips]?.[id]} (${id})`))

    const payload: Record<string, string> = {
      'First Name': this.firstName,
      'Last Name': this.lastName,
      Email: this.email,
      'Use Case': this.useCase,
      Persona: this.selectedPersona,
      'Selected States': this.selectedStates.map((f) => this.stateLookup[f]).join(', '),
      'Selected Counties': countyList.join(', '),
      'Selected Datasets': this.selectedUpgrades.join(', '),
      Timestamp: new Date().toISOString(),
    }

    const csv = convertToCsv([payload], Object.keys(payload))
    await uploadS3File(csv)
  }
}

window.addEventListener('DOMContentLoaded', () => {
  new ParquetMerge()
})
